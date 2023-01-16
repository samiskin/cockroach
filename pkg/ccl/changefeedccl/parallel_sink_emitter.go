package changefeedccl

import (
	"context"
	"fmt"
	"hash"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type flushingSink struct {
	emitter   parallelSinkEmitter
	successCh chan int

	inFlight int64
	flushCh  chan struct{}
	termCh   chan struct{}
	g        ctxgroup.Group
	mu       struct {
		syncutil.RWMutex
		shouldNotify bool
		termErr      error
	}
}

func makeFlushingSink(pse parallelSinkEmitter) *flushingSink {
	sink := flushingSink{
		emitter:   pse,
		successCh: make(chan int, 256),
		inFlight:  0,
		flushCh:   make(chan struct{}, 1),
		termCh:    make(chan struct{}, 1),
		g:         ctxgroup.WithContext(pse.ctx),
	}
	sink.g.GoCtx(func(ctx2 context.Context) error {
		return sink.emitConfirmationWorker(ctx2)
	})

	return &sink
}

func (fs *flushingSink) Close() error {
	_ = fs.emitter.Close()
	_ = fs.g.Wait()
	return nil
}

func (fs *flushingSink) Dial() error {
	return nil
}

func (fs *flushingSink) incInFlight() {
	// fs.mu.Lock()
	atomic.AddInt64(&fs.inFlight, 1)
	// fmt.Printf("\n\x1b[31m INC IN FLIGHT %d \x1b[0m\n", next)
	// fs.mu.Unlock()
}

func (fs *flushingSink) decInFlight(flushed int) {
	fs.mu.RLock()
	remaining := atomic.AddInt64(&fs.inFlight, -int64(flushed))
	// fmt.Printf("\n\x1b[31m DEC IN FLIGHT %d \x1b[0m\n", remaining)
	notifyFlush := remaining == 0 && fs.mu.shouldNotify
	fs.mu.RUnlock()
	// If shouldNotify is true, it is assumed that no new Emits could happen,
	// therefore it is not possible for this to occur multiple times for a single
	// Flush call
	if notifyFlush {
		fs.flushCh <- struct{}{}
	}
}

func (fs *flushingSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	// fmt.Printf("\n\x1b[31m EMIT ROW %s \x1b[0m\n", string(key))
	var topicName string
	var err error
	if fs.emitter.topicNamer != nil {
		topicName, err = fs.emitter.topicNamer.Name(topic)
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

	fs.incInFlight()
	fs.emitter.Emit(payload)

	return nil
}

func (fs *flushingSink) Flush(ctx context.Context) error {
	// fmt.Printf("\n\x1b[31m FLUSH \x1b[0m\n")

	fs.mu.Lock()
	if fs.inFlight == 0 {
		fs.mu.Unlock()
		return nil
	}
	fs.mu.shouldNotify = true
	fs.mu.Unlock()

	// fmt.Printf("\n\x1b[31m SENDING FLUSH \x1b[0m\n")
	flushPayload := newRowPayload()
	flushPayload.shouldFlush = true
	fs.emitter.Emit(flushPayload)

	select {
	case <-fs.emitter.ctx.Done():
		// fmt.Printf("\n\x1b[31m CONTEXT CANCELLATION \x1b[0m\n")
		return fs.emitter.ctx.Err()
	case <-fs.emitter.doneCh:
		// fmt.Printf("\n\x1b[31m DONE CH \x1b[0m\n")
		return nil
	case <-fs.termCh:
		fs.mu.RLock()
		defer fs.mu.RUnlock()
		return fs.mu.termErr
	case <-fs.flushCh:
		// fmt.Printf("\n\x1b[31m DONE FLUSH \x1b[0m\n")
		fs.mu.Lock()
		defer fs.mu.Unlock()
		fs.mu.shouldNotify = false
		return nil
	}
}

func (fs *flushingSink) emitConfirmationWorker(ctx context.Context) error {
	defer fmt.Printf("\n\x1b[31m EXITING WORKER LOOP \x1b[0m\n")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-fs.emitter.doneCh:
			return nil
		case err := <-fs.emitter.Errors():
			fs.mu.Lock()
			fmt.Printf("\n\x1b[31m ERROR FROM ERRORCH %s \x1b[0m\n", err.Error())
			if fs.mu.termErr == nil {
				fs.mu.termErr = err
				close(fs.termCh)
			}
			fs.mu.Unlock()
		case flushed := <-fs.emitter.Successes():
			fs.decInFlight(flushed)
		}
	}
}

func (fs *flushingSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	data, err := encoder.EncodeResolvedTimestamp(ctx, "", resolved)
	if err != nil {
		return err
	}

	var topics []string
	if fs.emitter.topicNamer == nil {
		topics = []string{""}
	} else {
		topics = fs.emitter.topicNamer.DisplayNamesSlice()
	}
	for _, topic := range topics {
		payload, err := fs.emitter.client.EncodeResolvedMessage(resolvedMessagePayload{
			resolvedTs: resolved,
			body:       data,
			topic:      topic,
		})
		if err != nil {
			return err
		}
		err = emitWithRetries(fs.emitter.ctx, topic, payload, 1, fs.emitter.client, fs.emitter.retryOpts, fs.emitter.metrics)
		if err != nil {
			return err
		}
	}
	return nil
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

func (pse *parallelSinkEmitter) Errors() chan error {
	return pse.errorCh
}

func (pse *parallelSinkEmitter) Successes() chan int {
	return pse.successCh
}

func (pse *parallelSinkEmitter) Close() error {
	close(pse.doneCh)
	_ = pse.wg.Wait()
	pse.pacer.Close()
	return pse.client.Close()
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
) Sink {
	pse := parallelSinkEmitter{
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

	// TODO: Make this its own makeFlushingSink
	return makeFlushingSink(pse)
}

func (pse *parallelSinkEmitter) Emit(payload *rowPayload) {
	if payload.shouldFlush {
		// fmt.Printf("\n\x1b[34m PARALLEL EMITTER FLUSH \x1b[0m\n")
		freeRowPayload(payload)
		for _, workerCh := range pse.workerCh {
			// Each worker requires its own message in order for them not to free the
			// message while it's being read by other workers
			flushPayload := newRowPayload()
			flushPayload.shouldFlush = true
			select {
			case <-pse.ctx.Done():
				return
			case <-pse.doneCh:
				return
			case workerCh <- flushPayload:
			}
		}
		return
	}

	// fmt.Printf("\n\x1b[34m PARALLEL EMITTER SEND \x1b[0m\n")
	workerId := pse.workerIndex(payload)
	pse.metrics.recordParallelEmitterAdmit()
	select {
	case <-pse.ctx.Done():
		return
	case <-pse.doneCh:
		return
	case pse.workerCh[workerId] <- payload:
		return
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
			if row.shouldFlush {
				// Each batcher requires its own message in order for them not to free the
				// message while it's being read by other batchers
				freeRowPayload(row)
				for _, emitter := range topicEmitters {
					flushPayload := newRowPayload()
					flushPayload.shouldFlush = true
					emitter.Emit(flushPayload)
				}
				continue
			}

			emitter, ok := topicEmitters[row.msg.topic]
			if !ok {
				emitter = makeTopicEmitter(row.msg.topic)
			}

			// fmt.Printf("\n\x1b[34m    PARALLEL WORKER SEND \x1b[0m\n")
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
