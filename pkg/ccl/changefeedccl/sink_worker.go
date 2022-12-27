package changefeedccl

import (
	"context"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type SinkWorkerFactory interface {
	MakeEventSink() (SinkWorker, error)
}

type sinkWorkerFactory struct {
	builder func() (SinkWorker, error)
}

var _ SinkWorkerFactory = (*sinkWorkerFactory)(nil)

func (swf *sinkWorkerFactory) MakeEventSink() (SinkWorker, error) {
	return swf.builder()
}

// ThinSink is the interface to an external sink used by a sink processor.
// Messages go through two steps, an Encoding step which creates a payload of
// one or more messages, then an Emit stage which sends the payload, with
// EmitPayload potentially being retried multiple times with the same payload if
// errors are observed.
type ThinSink interface {
	EncodeBatch([]MessagePayload) (SinkPayload, error)
	EncodeResolvedMessage(ResolvedMessagePayload) (SinkPayload, error)
	EmitPayload(SinkPayload) error
	Close() error
}

// SinkPayload is an interface representing a sink-specific representation of a
// batch of messages that is ready to be emitted by its EmitRow method.
type SinkPayload interface{}

// MessagePayload represents a KV event to be emitted.
type MessagePayload struct {
	key   []byte
	val   []byte
	topic TopicDescriptor // TODO move topicnamer into sink_worker
}

// ResolvedMessagePayload represents a Resolved event to be emitted.
type ResolvedMessagePayload struct {
	body       []byte
	resolvedTs hlc.Timestamp
}

type messageBatch struct {
	buffer      []MessagePayload
	bufferBytes int
	alloc       kvevent.Alloc
	mvcc        hlc.Timestamp
	bufferTime  time.Time // The earliest time a message was inserted into the batch
}

func newMessageBatch() messageBatch {
	return messageBatch{
		buffer:      make([]MessagePayload, 0),
		bufferBytes: 0,
	}
}

func (mb *messageBatch) isEmpty() bool {
	return len(mb.buffer) == 0
}

func (mb *messageBatch) reset() {
	mb.buffer = mb.buffer[:0]
	mb.bufferBytes = 0
	mb.alloc = kvevent.Alloc{}
}

type rowPayload struct {
	msg   MessagePayload
	alloc kvevent.Alloc
	mvcc  hlc.Timestamp
}

func (mb *messageBatch) moveIntoBuffer(p rowPayload, keyInValue bool) {
	if mb.isEmpty() {
		mb.bufferTime = timeutil.Now()
	}

	mb.buffer = append(mb.buffer, p.msg)
	mb.bufferBytes += len(p.msg.val)

	// Don't double-count the key bytes if the key is included in the value
	if !keyInValue {
		mb.bufferBytes += len(p.msg.key)
	}

	if mb.mvcc.IsEmpty() || p.mvcc.Less(mb.mvcc) {
		mb.mvcc = p.mvcc
	}

	mb.alloc.Merge(&p.alloc)
}

type batchWorkerMessage struct {
	sinkPayload SinkPayload
	numMessages int
	mvcc        hlc.Timestamp
	alloc       kvevent.Alloc
	kvBytes     int
	bufferTime  time.Time
}

type SinkWorker interface {
	Sink
	Successes() chan int
	Errors() chan error
}

type batchingWorkerSink struct {
	ctx context.Context

	ts         ThinSink
	config     batchConfig
	retryOpts  retry.Options
	timeSource timeutil.TimeSource

	successCh      chan int
	errorCh        chan error
	triggerFlushCh chan struct{}

	doneCh chan struct{}

	batchCh     chan batchWorkerMessage
	rowCh       chan rowPayload
	workerGroup ctxgroup.Group

	metrics metricsRecorder
}

var _ SinkWorker = (*batchingWorkerSink)(nil) // sinkProcessor should implement Sink

// Flush is a no-op to be compliant with Sink, users are expected to rely on
// Successes() and Errors()
func (bs *batchingWorkerSink) Flush(ctx context.Context) error {
	return nil
}

func (bs *batchingWorkerSink) Dial() error {
	return nil
}

func (bs *batchingWorkerSink) Successes() chan int {
	return bs.successCh
}

func (bs *batchingWorkerSink) Errors() chan error {
	return bs.errorCh
}

func makeBatchingWorkerSink(
	ctx context.Context, sink ThinSink, config batchConfig, retryOpts retry.Options, timeSource timeutil.TimeSource, mb metricsRecorderBuilder,
) (SinkWorker, error) {
	bs := batchingWorkerSink{
		ctx:       ctx,
		ts:        sink,
		batchCh:   make(chan batchWorkerMessage, 256),
		rowCh:     make(chan rowPayload, 256),
		successCh: make(chan int, 256),
		errorCh:   make(chan error, 256),

		triggerFlushCh: make(chan struct{}),
		doneCh:         make(chan struct{}),
		workerGroup:    ctxgroup.WithContext(ctx),
		timeSource:     timeSource,
		config:         config,
		retryOpts:      retryOpts,
		metrics:        mb(requiresResourceAccounting),
	}

	// Since flushes need to be triggerable from both EmitRow and a timer firing,
	// they must be done in a dedicated goroutine.
	bs.workerGroup.GoCtx(func(ctx context.Context) error {
		return bs.startBatchWorker()
	})

	// Since messages should be able to be batched while a batch is in-flight to
	// the downstream sink, emitting is done in its own goroutine.
	bs.workerGroup.GoCtx(func(ctx context.Context) error {
		return bs.startEmitWorker()
	})

	return &bs, nil
}

func (bs *batchingWorkerSink) Close() error {
	close(bs.doneCh)
	// Ignore errors related to outstanding messages since we're either shutting
	// down or beginning to retry regardless
	_ = bs.workerGroup.Wait()
	return nil
}

func (bs *batchingWorkerSink) handleError(err error) {
	bs.errorCh <- changefeedbase.MarkRetryableError(err)
}

func (bs *batchingWorkerSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	bs.rowCh <- rowPayload{
		msg: MessagePayload{
			key:   key,
			val:   value,
			topic: topic,
		},
		mvcc:  mvcc,
		alloc: alloc,
	}
	return nil
}

func (bs *batchingWorkerSink) startBatchWorker() error {
	currentBatch := newMessageBatch()

	flushBatch := func() {
		if currentBatch.isEmpty() {
			return
		}

		// Reuse the same batch to avoid need for garbage collection
		defer currentBatch.reset()

		// Process messages into a payload ready to be emitted to the sink
		sinkPayload, err := bs.ts.EncodeBatch(currentBatch.buffer)
		if err != nil {
			bs.handleError(err)
			return
		}

		// Send the encoded batch to a separate worker so that flushes do not block
		// further message aggregation
		bs.batchCh <- batchWorkerMessage{
			sinkPayload: sinkPayload,
			alloc:       currentBatch.alloc,
			numMessages: len(currentBatch.buffer),
			mvcc:        currentBatch.mvcc,
			kvBytes:     currentBatch.bufferBytes,
			bufferTime:  currentBatch.bufferTime,
		}
	}

	flushTimer := bs.timeSource.NewTimer()

	for {
		select {
		case <-bs.ctx.Done():
			return bs.ctx.Err()
		case <-bs.doneCh:
			return nil
		case rowMsg := <-bs.rowCh:
			bs.metrics.recordMessageSize(int64(len(rowMsg.msg.key) + len(rowMsg.msg.val)))

			// If the batch is about to no longer be empty, start the flush timer
			if currentBatch.isEmpty() && time.Duration(bs.config.Frequency) > 0 {
				flushTimer.Reset(time.Duration(bs.config.Frequency))
			}
			currentBatch.moveIntoBuffer(rowMsg, false) // TODO: Key in value

			if bs.shouldFlushBatch(currentBatch) {
				bs.metrics.recordSizeBasedFlush()
				flushBatch()
			}
		case <-bs.triggerFlushCh:
			flushBatch()
		case <-flushTimer.Ch():
			flushTimer.MarkRead()
			flushBatch()
		}
	}
}

func (bs *batchingWorkerSink) shouldFlushBatch(batch messageBatch) bool {
	switch {
	// all zero values is interpreted as flush every time
	case bs.config.Messages == 0 && bs.config.Bytes == 0 && bs.config.Frequency == 0:
		return true
	// messages threshold has been reached
	case bs.config.Messages > 0 && len(batch.buffer) >= bs.config.Messages:
		return true
	// bytes threshold has been reached
	case bs.config.Bytes > 0 && batch.bufferBytes >= bs.config.Bytes:
		return true
	default:
		return false
	}
}

func (bs *batchingWorkerSink) startEmitWorker() error {
	for {
		select {
		case <-bs.ctx.Done():
			return bs.ctx.Err()
		case <-bs.doneCh:
			return nil
		case batch := <-bs.batchCh:
			flushCallback := bs.metrics.recordFlushRequestCallback()
			err := bs.sendWithRetries(batch.sinkPayload, batch.numMessages)
			if err != nil {
				bs.handleError(err)
				return nil
			}

			bs.metrics.recordEmittedBatch(
				batch.bufferTime, batch.numMessages, batch.mvcc, batch.kvBytes, sinkDoesNotCompress)
			batch.alloc.Release(bs.ctx)

			bs.successCh <- batch.numMessages
			flushCallback()
		}
	}
}

func (bs *batchingWorkerSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	defer bs.metrics.recordResolvedCallback()()

	data, err := encoder.EncodeResolvedTimestamp(ctx, "", resolved)
	if err != nil {
		return err
	}
	payload, err := bs.ts.EncodeResolvedMessage(ResolvedMessagePayload{
		resolvedTs: resolved,
		body:       data,
	})
	if err != nil {
		return err
	}
	return bs.sendWithRetries(payload, 1)
}

func (bs *batchingWorkerSink) sendWithRetries(payload SinkPayload, numMessages int) error {
	initialSend := true
	return retry.WithMaxAttempts(bs.ctx, bs.retryOpts, bs.retryOpts.MaxRetries+1, func() error {
		if !initialSend {
			bs.metrics.recordInternalRetry(int64(numMessages), false)
		}
		err := bs.ts.EmitPayload(payload)
		initialSend = false
		return err
	})
}

type batchConfig struct {
	Bytes, Messages int          `json:",omitempty"`
	Frequency       jsonDuration `json:",omitempty"`
}

// wrapper structs to unmarshal json, retry.Options will be the actual config
type retryConfigJson struct {
	Max     jsonMaxRetries `json:",omitempty"`
	Backoff jsonDuration   `json:",omitempty"`
}

type jsonMaxRetries int

func (j *jsonMaxRetries) UnmarshalJSON(b []byte) error {
	var i int64
	// try to parse as int
	i, err := strconv.ParseInt(string(b), 10, 64)
	if err == nil {
		if i <= 0 {
			return errors.Errorf("max retry count must be a positive integer. use 'inf' for infinite retries.")
		}
		*j = jsonMaxRetries(i)
	} else {
		// if that fails, try to parse as string (only accept 'inf')
		var s string
		// using unmarshal here to remove quotes around the string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		if strings.ToLower(s) == "inf" {
			// if used wants infinite retries, set to zero as retry.Options interprets this as infinity
			*j = 0
		} else if n, err := strconv.Atoi(s); err == nil { // also accept ints as strings
			*j = jsonMaxRetries(n)
		} else {
			return errors.Errorf("max retries must be either a positive int or 'inf' for infinite retries.")
		}
	}
	return nil
}

// proper JSON schema for sink config:
//
//	{
//	  "Flush": {
//		   "Messages":  ...,
//		   "Bytes":     ...,
//		   "Frequency": ...,
//	  },
//		 "Retry": {
//		   "Max":     ...,
//		   "Backoff": ...,
//	  }
//	}
type batchingWorkerConfigJson struct {
	Flush batchConfig     `json:",omitempty"`
	Retry retryConfigJson `json:",omitempty"`
}

func defaultRetryConfig() retry.Options {
	opts := retry.Options{
		InitialBackoff: 500 * time.Millisecond,
		MaxRetries:     3,
		Multiplier:     2,
	}
	// max backoff should be initial * 2 ^ maxRetries
	opts.MaxBackoff = opts.InitialBackoff * time.Duration(int(math.Pow(2.0, float64(opts.MaxRetries))))
	return opts
}

func getWorkerConfigFromJson(
	jsonStr changefeedbase.SinkSpecificJSONConfig,
) (batchCfg batchConfig, retryCfg retry.Options, err error) {
	retryCfg = defaultRetryConfig()

	var cfg batchingWorkerConfigJson
	cfg.Retry.Max = jsonMaxRetries(retryCfg.MaxRetries)
	cfg.Retry.Backoff = jsonDuration(retryCfg.InitialBackoff)
	if jsonStr != `` {
		// set retry defaults to be overridden if included in JSON
		if err = json.Unmarshal([]byte(jsonStr), &cfg); err != nil {
			return batchCfg, retryCfg, errors.Wrapf(err, "error unmarshalling json")
		}
	}

	// don't support negative values
	if cfg.Flush.Messages < 0 || cfg.Flush.Bytes < 0 || cfg.Flush.Frequency < 0 ||
		cfg.Retry.Max < 0 || cfg.Retry.Backoff < 0 {
		return batchCfg, retryCfg, errors.Errorf("invalid option value %s, all config values must be non-negative", changefeedbase.OptWebhookSinkConfig)
	}

	// errors if other batch values are set, but frequency is not
	if (cfg.Flush.Messages > 0 || cfg.Flush.Bytes > 0) && cfg.Flush.Frequency == 0 {
		return batchCfg, retryCfg, errors.Errorf("invalid option value %s, flush frequency is not set, messages may never be sent", changefeedbase.OptWebhookSinkConfig)
	}

	retryCfg.MaxRetries = int(cfg.Retry.Max)
	retryCfg.InitialBackoff = time.Duration(cfg.Retry.Backoff)
	return cfg.Flush, retryCfg, nil
}
