package changefeedccl

import (
	"context"
	"hash"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

type AsyncSink interface {
	Sink
	Successes() chan int
	Errors() chan error
}

type parallelSinkEmitter struct {
	ctx context.Context

	client SinkClient

	batchCfg  sinkBatchConfig
	retryOpts retry.Options

	successCh chan int
	errorCh   chan error

	topicNamer    *TopicNamer
	topicEmitters map[string]parallelTopicEmitter

	wg      ctxgroup.Group
	doneCh  chan struct{}
	metrics metricsRecorder
}

var _ AsyncSink = (*parallelSinkEmitter)(nil)

func (pse *parallelSinkEmitter) Errors() chan error {
	return pse.errorCh
}

func (pse *parallelSinkEmitter) Successes() chan int {
	return pse.successCh
}

func (pse *parallelSinkEmitter) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	// Just insert into the appropiate topic producer
	return nil
}

func (pse *parallelSinkEmitter) Flush(ctx context.Context) error {
	// Tell each topic producer to flush
	return nil
}

func (pse *parallelSinkEmitter) Close() error {
	close(pse.doneCh)
	return pse.wg.Wait()
}

func (pse *parallelSinkEmitter) Dial() error {
	return nil
}

type parallelTopicEmitter struct {
	ctx        context.Context
	numWorkers int64
	workerCh   []chan rowPayload
	hasher     hash.Hash32
	wg         ctxgroup.Group
	doneCh     chan struct{}
}

func makeTopicEmitter(
	ctx context.Context,
	numWorkers int64,
	emitterFactory func() batchedSinkEmitter,
) *parallelTopicEmitter {
	pte := &parallelTopicEmitter{
		ctx:        ctx,
		numWorkers: numWorkers,
		workerCh:   make([]chan rowPayload, numWorkers),
		hasher:     makeHasher(),
		wg:         ctxgroup.WithContext(ctx),
		doneCh:     make(chan struct{}),
	}

	for worker := int64(0); worker < pte.numWorkers; worker++ {
		workerCh := make(chan rowPayload, 256)
		emitter := emitterFactory()
		pte.wg.GoCtx(func(ctx context.Context) error {
			return pte.workerLoop(workerCh, emitter)
		})
		pte.workerCh[worker] = workerCh
	}

	return pte
}

func (pte *parallelTopicEmitter) workerLoop(input chan rowPayload, emitter batchedSinkEmitter) error {
	for {
		select {
		case <-pte.ctx.Done():
			return pte.ctx.Err()
		case <-pte.doneCh:
			return nil
		case row := <-input:
			emitter.Emit(row)
		}
	}
	return nil
}

func (pte *parallelTopicEmitter) Emit(payload rowPayload) {
	workerId := pte.workerIndex(payload)
	select {
	case <-pte.ctx.Done():
		return
	case <-pte.doneCh:
		return
	case pte.workerCh[workerId] <- payload:
		return
	}
}

func (pte *parallelTopicEmitter) workerIndex(row rowPayload) int64 {
	pte.hasher.Reset()
	pte.hasher.Write(row.msg.key)
	return int64(pte.hasher.Sum32()) % pte.numWorkers
}

func (pse *parallelSinkEmitter) EmitResolvedTimestamp(ctx context.Context, encoder Encoder, resolved hlc.Timestamp) error {
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
		err = emitWithRetries(pse.ctx, payload, 1, pse.client, pse.retryOpts, pse.metrics)
		if err != nil {
			return err
		}
	}
	return nil
}

// TODO: These goes into the parallel sink
// func (bs *batchedSinkEmitter) EmitRow(
// 	ctx context.Context,
// 	topic TopicDescriptor,
// 	key, value []byte,
// 	updated, mvcc hlc.Timestamp,
// 	alloc kvevent.Alloc,
// ) error {
// 	bs.rowCh <- rowPayload{
// 		msg: messagePayload{
// 			key:   key,
// 			val:   value,
// 			topic: topic,
// 		},
// 		mvcc:  mvcc,
// 		alloc: alloc,
// 	}
// 	return nil
// }
//
// func (bs *batchedSinkEmitter) EmitResolvedTimestamp(
// 	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
// ) error {
// 	defer bs.metrics.recordResolvedCallback()()
//
// 	data, err := encoder.EncodeResolvedTimestamp(ctx, "", resolved)
// 	if err != nil {
// 		return err
// 	}
// 	payload, err := bs.se.EncodeResolvedMessage(resolvedMessagePayload{
// 		resolvedTs: resolved,
// 		body:       data,
// 	})
// 	if err != nil {
// 		return err
// 	}
// 	return bs.sendWithRetries(payload, 1)
// }
//
