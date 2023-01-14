package changefeedccl

import (
	"context"
	"hash"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type AsyncSink interface {
	Sink
	Successes() chan int
	Errors() chan error
}

type rowPayload struct {
	msg         messagePayload
	alloc       kvevent.Alloc
	mvcc        hlc.Timestamp
	topic       TopicDescriptor
	shouldFlush bool
}

var rowPayloadPool = sync.Pool{
	New: func() interface{} {
		return new(rowPayload)
	},
}

func newRowPayload() *rowPayload {
	return rowPayloadPool.Get().(*rowPayload)
}

func freeRowPayload(r *rowPayload) {
	*r = rowPayload{}
	rowPayloadPool.Put(r)
}

type parallelSinkEmitter struct {
	ctx context.Context

	client    SinkClient
	batchCfg  sinkBatchConfig
	retryOpts retry.Options
	successCh chan int
	errorCh   chan error

	topicNamer *TopicNamer

	workerCh   []chan *rowPayload
	numWorkers int64
	hasher     hash.Hash32

	wg      ctxgroup.Group
	doneCh  chan struct{}
	metrics metricsRecorder
	pacer   SinkPacer
}

var _ AsyncSink = (*parallelSinkEmitter)(nil)

func (pse *parallelSinkEmitter) Errors() chan error {
	return pse.errorCh
}

func (pse *parallelSinkEmitter) Successes() chan int {
	return pse.successCh
}

func (pse *parallelSinkEmitter) Flush(ctx context.Context) error {
	// Tell each topic producer to flush
	flushPayload := newRowPayload()
	flushPayload.shouldFlush = true
	for _, workerCh := range pse.workerCh {
		select {
		case <-pse.ctx.Done():
			return pse.ctx.Err()
		case <-pse.doneCh:
			return nil
		case workerCh <- flushPayload:
			return nil
		}
	}
	return nil
}

func (pse *parallelSinkEmitter) Close() error {
	close(pse.doneCh)
	_ = pse.wg.Wait()
	pse.pacer.Close()
	return pse.client.Close()
}

func (pse *parallelSinkEmitter) Dial() error {
	return nil
}

func makeParallelSinkEmitter(
	ctx context.Context,
	client SinkClient,
	config sinkBatchConfig,
	retryOpts retry.Options,
	numWorkers int64,
	topicNamer *TopicNamer,
	metrics metricsRecorder,
	pacer SinkPacer,
) AsyncSink {
	pse := &parallelSinkEmitter{
		ctx:        ctx,
		client:     client,
		batchCfg:   config,
		retryOpts:  retryOpts,
		topicNamer: topicNamer,

		successCh: make(chan int, 256),
		errorCh:   make(chan error, 1),

		workerCh:   make([]chan *rowPayload, numWorkers),
		numWorkers: numWorkers,
		hasher:     makeHasher(),

		wg:      ctxgroup.WithContext(ctx),
		doneCh:  make(chan struct{}),
		metrics: metrics,
		pacer:   pacer,
	}

	for worker := int64(0); worker < pse.numWorkers; worker++ {
		workerCh := make(chan *rowPayload, 256)
		pse.wg.GoCtx(func(ctx context.Context) error {
			return pse.workerLoop(workerCh)
		})
		pse.workerCh[worker] = workerCh
	}

	return pse
}

func (pse *parallelSinkEmitter) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	var topicName string
	var err error
	if pse.topicNamer != nil {
		topicName, err = pse.topicNamer.Name(topic)
		if err != nil {
			return err
		}
	}

	payload := newRowPayload()
	payload.msg = messagePayload{
		key:   key,
		val:   value,
		topic: topicName,
	}
	payload.mvcc = mvcc
	payload.alloc = alloc
	payload.topic = topic

	workerId := pse.workerIndex(payload)
	pse.metrics.recordParallelEmitterAdmit()
	select {
	case <-pse.ctx.Done():
		return pse.ctx.Err()
	case <-pse.doneCh:
		return nil
	case pse.workerCh[workerId] <- payload:
		return nil
	}
}

func (pse *parallelSinkEmitter) workerLoop(input chan *rowPayload) error {
	// Since topics usually have their own endpoints to flush to, they can each be
	// their own batcher
	topicEmitters := make(map[string]*batchedSinkEmitter)
	makeTopicEmitter := func(topic string) *batchedSinkEmitter {
		emitter := makeBatchedSinkEmitter(
			pse.ctx,
			topic,
			pse.client,
			pse.batchCfg,
			pse.retryOpts,
			pse.successCh,
			pse.errorCh,
			timeutil.DefaultTimeSource{},
			pse.metrics,
			pse.pacer,
		)
		topicEmitters[topic] = emitter
		return emitter
	}
	defer func() {
		for _, emitter := range topicEmitters {
			_ = emitter.Close()
		}
	}()

	for {
		pse.pacer.Pace(pse.ctx)

		select {
		case <-pse.ctx.Done():
			return pse.ctx.Err()
		case <-pse.doneCh:
			return nil
		case row := <-input:
			emitter, ok := topicEmitters[row.msg.topic]
			if !ok {
				emitter = makeTopicEmitter(row.msg.topic)
			}

			emitter.Emit(row)
			pse.metrics.recordParallelEmitterEmit()
		}
	}
}

func (pse *parallelSinkEmitter) workerIndex(row *rowPayload) int64 {
	pse.hasher.Reset()
	pse.hasher.Write(row.msg.key)
	return int64(pse.hasher.Sum32()) % pse.numWorkers
}

func (pse *parallelSinkEmitter) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	data, err := encoder.EncodeResolvedTimestamp(ctx, "", resolved)
	if err != nil {
		return err
	}

	var topics []string
	if pse.topicNamer == nil {
		topics = []string{""}
	} else {
		topics = pse.topicNamer.DisplayNamesSlice()
	}
	for _, topic := range topics {
		payload, err := pse.client.EncodeResolvedMessage(resolvedMessagePayload{
			resolvedTs: resolved,
			body:       data,
			topic:      topic,
		})
		if err != nil {
			return err
		}
		err = emitWithRetries(pse.ctx, topic, payload, 1, pse.client, pse.retryOpts, pse.metrics)
		if err != nil {
			return err
		}
	}
	return nil
}
