package changefeedccl

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type parallelBatchingSink struct {
	emitter FlushingEmitter

	client     SinkClient
	topicNamer *TopicNamer
}

func makeParallelBatchingSink(
	ctx context.Context,
	client SinkClient,
	batchCfg sinkBatchConfig,
	retryOpts retry.Options,
	numWorkers int64,
	topicNamer *TopicNamer,
	timeSource timeutil.TimeSource,
	metrics metricsRecorder,
	pacer SinkPacer,
) Sink {
	batchingEmitterFactory := func(topic string, successCh chan int, errorCh chan error) SinkEmitter {
		return makeBatchingSinkEmitter(
			ctx,
			client,
			batchCfg,
			retryOpts,
			topic,
			successCh,
			errorCh,
			timeSource,
			metrics,
			pacer,
		)
	}

	parallelEmitter := makeParallelSinkEmitter(
		ctx,
		batchingEmitterFactory,
		numWorkers,
		metrics,
		pacer,
	)

	flushingEmitter := makeFlushingEmitter(
		ctx,
		parallelEmitter,
	)

	return &parallelBatchingSink{
		emitter:    flushingEmitter,
		client:     client,
		topicNamer: topicNamer,
	}
}

var _ Sink = (*parallelBatchingSink)(nil)

func (ps *parallelBatchingSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	var topicName string
	var err error
	if ps.topicNamer != nil {
		topicName, err = ps.topicNamer.Name(topic)
		if err != nil {
			return err
		}
	}

	payload := newSinkEvent()
	payload.msg = messagePayload{
		key:   key,
		val:   value,
		topic: topicName,
	}
	payload.mvcc = mvcc
	payload.alloc = alloc
	ps.emitter.Emit(payload)

	return nil
}

func (ps *parallelBatchingSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	data, err := encoder.EncodeResolvedTimestamp(ctx, "", resolved)
	if err != nil {
		return err
	}

	var topics []string
	if ps.topicNamer == nil {
		topics = []string{""}
	} else {
		topics = ps.topicNamer.DisplayNamesSlice()
	}
	for _, topic := range topics {
		event := newSinkEvent()
		event.resolved = &resolvedMessagePayload{
			body:  data,
			topic: topic,
		}
		ps.emitter.Emit(event)
	}
	return ps.Flush(ctx)
}

func (ps *parallelBatchingSink) Flush(ctx context.Context) error {
	return ps.emitter.Flush()
}

func (ps *parallelBatchingSink) Close() error {
	ps.emitter.Close()
	return ps.client.Close()
}

func (ps *parallelBatchingSink) Dial() error {
	return nil
}

type sinkEvent struct {
	// Set if its a Flush event
	shouldFlush bool

	// Set if its a Resolved event
	resolved *resolvedMessagePayload

	// Set if its a KV Event
	msg   messagePayload
	alloc kvevent.Alloc
	mvcc  hlc.Timestamp
}

var sinkEventPool = sync.Pool{
	New: func() interface{} {
		return new(sinkEvent)
	},
}

func newSinkEvent() *sinkEvent {
	return sinkEventPool.Get().(*sinkEvent)
}

func freeSinkEvent(e *sinkEvent) {
	*e = sinkEvent{}
	sinkEventPool.Put(e)
}

func newSinkFlushEvent() *sinkEvent {
	e := newSinkEvent()
	e.shouldFlush = true
	return e
}
