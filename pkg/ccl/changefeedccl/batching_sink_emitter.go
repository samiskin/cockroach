package changefeedccl

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type SinkEmitter interface {
	Emit(*sinkEvent)
	Close() error
}

type batchingSinkEmitter struct {
	ctx   context.Context
	sc    SinkClient
	topic string

	batchCfg  sinkBatchConfig
	retryOpts retry.Options

	successCh chan int
	errorCh   chan error

	rowCh chan *sinkEvent

	mu struct {
		syncutil.RWMutex
		termErr error
	}
	doneCh     chan struct{}
	wg         ctxgroup.Group
	timeSource timeutil.TimeSource
	metrics    metricsRecorder
	pacer      SinkPacer
}

var _ SinkEmitter = (*batchingSinkEmitter)(nil)

func makeBatchingSinkEmitter(
	ctx context.Context,
	sink SinkClient,
	config sinkBatchConfig,
	retryOpts retry.Options,
	topic string,
	successCh chan int,
	errorCh chan error,
	timeSource timeutil.TimeSource,
	metrics metricsRecorder,
	pacer SinkPacer,
) *batchingSinkEmitter {
	bs := batchingSinkEmitter{
		ctx:       ctx,
		sc:        sink,
		topic:     topic,
		batchCfg:  config,
		retryOpts: retryOpts,

		successCh: successCh,
		errorCh:   errorCh,

		rowCh:      make(chan *sinkEvent, 64),
		doneCh:     make(chan struct{}),
		wg:         ctxgroup.WithContext(ctx),
		timeSource: timeSource,
		metrics:    metrics,
		pacer:      pacer,
	}

	bs.wg.GoCtx(func(ctx context.Context) error {
		return bs.startBatchWorker()
	})

	return &bs
}

func (bse *batchingSinkEmitter) Errors() chan error {
	return bse.errorCh
}

func (bse *batchingSinkEmitter) Successes() chan int {
	return bse.successCh
}

var batchWorkerMessagePool = sync.Pool{
	New: func() interface{} {
		return new(batchWorkerMessage)
	},
}

func newBatchWorkerMessage(payload SinkPayload, batch *messageBatch) *batchWorkerMessage {
	message := batchWorkerMessagePool.Get().(*batchWorkerMessage)
	message.sinkPayload = payload
	message.alloc = batch.alloc
	message.numMessages = len(batch.buffer)
	message.mvcc = batch.mvcc
	message.kvBytes = batch.bufferBytes
	message.bufferTime = batch.bufferTime
	return message
}

func freeBatchWorkerMessage(r *batchWorkerMessage) {
	*r = batchWorkerMessage{}
	batchWorkerMessagePool.Put(r)
}

func (bs *batchingSinkEmitter) Emit(payload *sinkEvent) {
	bs.metrics.recordBatchingEmitterAdmit()
	select {
	case <-bs.ctx.Done():
		return
	case <-bs.doneCh:
		return
	case bs.rowCh <- payload:
		return
	}
}

func (bs *batchingSinkEmitter) Close() error {
	close(bs.doneCh)
	return bs.wg.Wait()
}

func (bs *batchingSinkEmitter) handleError(err error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if bs.mu.termErr == nil {
		bs.mu.termErr = err
	} else {
		return
	}

	select {
	case <-bs.ctx.Done():
		return
	case <-bs.doneCh:
		return
	case bs.errorCh <- bs.mu.termErr:
		return
	}
}

type batchWorkerMessage struct {
	sinkPayload SinkPayload
	numMessages int
	mvcc        hlc.Timestamp
	alloc       kvevent.Alloc
	kvBytes     int
	bufferTime  time.Time
}

func (bs *batchingSinkEmitter) startBatchWorker() error {
	currentBatch := newMessageBatch()

	// Emitting a batch is a blocking I/O operation so performing it in its own
	// goroutine allows for the next batch to be constructed and queued up in
	// parallel.
	batchCh := make(chan *batchWorkerMessage, 64)
	bs.wg.GoCtx(func(ctx context.Context) error {
		return bs.startEmitWorker(batchCh)
	})

	flushBatch := func() {
		if currentBatch.isEmpty() {
			return
		}

		// Reuse the same batch to avoid need for garbage collection
		defer currentBatch.reset()

		// Process messages into a payload ready to be emitted to the sink
		sinkPayload, err := bs.sc.EncodeBatch(currentBatch.buffer)
		if err != nil {
			bs.handleError(err)
			return
		}

		// Send the encoded batch to a separate worker so that flushes do not block
		// further message aggregation
		select {
		case <-bs.ctx.Done():
			return
		case <-bs.doneCh:
			return
		case batchCh <- newBatchWorkerMessage(sinkPayload, &currentBatch):
			return
		}
	}

	flushTimer := bs.timeSource.NewTimer()

	for {
		bs.pacer.Pace(bs.ctx)

		select {
		case <-bs.ctx.Done():
			return bs.ctx.Err()
		case <-bs.doneCh:
			return nil
		case sinkEvent := <-bs.rowCh:
			defer freeSinkEvent(sinkEvent)

			if bs.isTerminated() {
				continue
			}
			if sinkEvent.shouldFlush {
				flushBatch()
				continue
			}
			// Resolved messages aren't batched with normal messages
			if sinkEvent.resolved != nil {
				flushBatch()
				bs.emitResolvedEvent(sinkEvent.resolved.body)
				continue
			}

			bs.metrics.recordMessageSize(int64(len(sinkEvent.msg.key) + len(sinkEvent.msg.val)))

			// If the batch is about to no longer be empty, start the flush timer
			if currentBatch.isEmpty() && time.Duration(bs.batchCfg.Frequency) > 0 {
				flushTimer.Reset(time.Duration(bs.batchCfg.Frequency))
			}
			currentBatch.Append(sinkEvent, false) // TODO: Key in value

			if bs.shouldFlushBatch(currentBatch) {
				bs.metrics.recordSizeBasedFlush()
				flushBatch()
			}
		case <-flushTimer.Ch():
			flushTimer.MarkRead()
			flushBatch()
		}
	}
}

func (bs *batchingSinkEmitter) emitResolvedEvent(resolvedBody []byte) {
	payload, err := bs.sc.EncodeResolvedMessage(resolvedMessagePayload{
		body:  resolvedBody,
		topic: bs.topic,
	})
	if err != nil {
		bs.handleError(err)
		return
	}

	err = emitWithRetries(bs.ctx, bs.topic, payload, 1, bs.sc, bs.retryOpts, bs.metrics)
	if err != nil {
		bs.handleError(err)
		return
	}
	bs.successCh <- 1
}

func (bs *batchingSinkEmitter) isTerminated() bool {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	return bs.mu.termErr != nil
}

func (bs *batchingSinkEmitter) startEmitWorker(batchCh chan *batchWorkerMessage) error {
	for {
		select {
		case <-bs.ctx.Done():
			return bs.ctx.Err()
		case <-bs.doneCh:
			return nil
		case batch := <-batchCh:
			// Never emit messages if an error has occured as ordering guarantees may be compromised
			if bs.isTerminated() {
				continue
			}

			flushCallback := bs.metrics.recordFlushRequestCallback()
			err := emitWithRetries(bs.ctx, bs.topic, batch.sinkPayload, batch.numMessages, bs.sc, bs.retryOpts, bs.metrics)
			if err != nil {
				bs.handleError(err)
				return nil
			}

			bs.metrics.recordBatchingEmitterEmit(batch.numMessages)
			bs.metrics.recordEmittedBatch(
				batch.bufferTime, batch.numMessages, batch.mvcc, batch.kvBytes, sinkDoesNotCompress)
			batch.alloc.Release(bs.ctx)

			bs.successCh <- batch.numMessages
			freeBatchWorkerMessage(batch)
			flushCallback()
		}
	}
}

func (bs *batchingSinkEmitter) shouldFlushBatch(batch messageBatch) bool {
	switch {
	// all zero values is interpreted as flush every time
	case bs.batchCfg.Messages == 0 && bs.batchCfg.Bytes == 0 && bs.batchCfg.Frequency == 0:
		return true
	// messages threshold has been reached
	case bs.batchCfg.Messages > 0 && len(batch.buffer) >= bs.batchCfg.Messages:
		return true
	// bytes threshold has been reached
	case bs.batchCfg.Bytes > 0 && batch.bufferBytes >= bs.batchCfg.Bytes:
		return true
	default:
		return false
	}
}

type messageBatch struct {
	buffer      []messagePayload
	bufferBytes int
	alloc       kvevent.Alloc
	mvcc        hlc.Timestamp
	bufferTime  time.Time // The earliest time a message was inserted into the batch
}

func newMessageBatch() messageBatch {
	return messageBatch{
		buffer:      make([]messagePayload, 0),
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

func (mb *messageBatch) Append(p *sinkEvent, keyNotEmitted bool) {
	if mb.isEmpty() {
		mb.bufferTime = timeutil.Now()
	}

	mb.buffer = append(mb.buffer, p.msg)
	mb.bufferBytes += len(p.msg.val)

	// Some sinks (ex: webhook) include the key in the value and only emit the
	// value, therefore in those cases the bytes of the key shouldn't be counted
	if !keyNotEmitted {
		mb.bufferBytes += len(p.msg.key)
	}

	if mb.mvcc.IsEmpty() || p.mvcc.Less(mb.mvcc) {
		mb.mvcc = p.mvcc
	}

	mb.alloc.Merge(&p.alloc)
}
