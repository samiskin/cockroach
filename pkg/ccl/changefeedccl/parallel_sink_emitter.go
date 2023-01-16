package changefeedccl

import (
	"context"
	"hash"

	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
)

type AsyncEmitter interface {
	SinkEmitter
	Successes() chan int
	Errors() chan error
}

type parallelSinkEmitter struct {
	ctx                 context.Context
	topicEmitterFactory TopicEmitterFactory

	successCh chan int
	errorCh   chan error

	workerCh   []chan *sinkEvent
	numWorkers int64
	hasher     hash.Hash32

	wg      ctxgroup.Group
	doneCh  chan struct{}
	metrics metricsRecorder
	pacer   SinkPacer
}

var _ AsyncEmitter = (*parallelSinkEmitter)(nil)

func (pse *parallelSinkEmitter) Errors() chan error {
	return pse.errorCh
}

func (pse *parallelSinkEmitter) Successes() chan int {
	return pse.successCh
}

func (pse *parallelSinkEmitter) Close() error {
	close(pse.doneCh)
	err := pse.wg.Wait()
	pse.pacer.Close()
	return err
}

type TopicEmitterFactory = func(topic string, successCh chan int, errorCh chan error) SinkEmitter

func makeParallelSinkEmitter(
	ctx context.Context,
	topicEmitterFactory TopicEmitterFactory,
	numWorkers int64,
	metrics metricsRecorder,
	pacer SinkPacer,
) AsyncEmitter {
	pse := parallelSinkEmitter{
		ctx:                 ctx,
		topicEmitterFactory: topicEmitterFactory,

		successCh: make(chan int, 256),
		errorCh:   make(chan error, 1),

		workerCh:   make([]chan *sinkEvent, numWorkers),
		numWorkers: numWorkers,
		hasher:     makeHasher(),

		wg:      ctxgroup.WithContext(ctx),
		doneCh:  make(chan struct{}),
		metrics: metrics,
		pacer:   pacer,
	}

	for worker := int64(0); worker < pse.numWorkers; worker++ {
		workerCh := make(chan *sinkEvent, 256)
		pse.wg.GoCtx(func(ctx context.Context) error {
			return pse.workerLoop(workerCh)
		})
		pse.workerCh[worker] = workerCh
	}

	return &pse
}

func (pse *parallelSinkEmitter) Emit(payload *sinkEvent) {
	if payload.shouldFlush {
		// Each worker requires its own message in order for them not to free the
		// message while it's being read by other workers
		freeSinkEvent(payload)
		for _, workerCh := range pse.workerCh {
			select {
			case <-pse.ctx.Done():
				return
			case <-pse.doneCh:
				return
			case workerCh <- newSinkFlushEvent():
			}
		}
		return
	}

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

func (pse *parallelSinkEmitter) workerLoop(input chan *sinkEvent) error {
	// Since topics usually have their own endpoints to flush to, they can each be
	// their own batcher
	topicEmitters := make(map[string]SinkEmitter)
	makeTopicEmitter := func(topic string) SinkEmitter {
		emitter := pse.topicEmitterFactory(topic, pse.successCh, pse.errorCh)
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
				freeSinkEvent(row)
				for _, emitter := range topicEmitters {
					emitter.Emit(newSinkFlushEvent())
				}
				continue
			}

			emitter, ok := topicEmitters[row.msg.topic]
			if !ok {
				emitter = makeTopicEmitter(row.msg.topic)
			}

			emitter.Emit(row)
			pse.metrics.recordParallelEmitterEmit()
		}
	}
}

func (pse *parallelSinkEmitter) workerIndex(row *sinkEvent) int64 {
	pse.hasher.Reset()
	pse.hasher.Write(row.msg.key)
	return int64(pse.hasher.Sum32()) % pse.numWorkers
}
