package changefeedccl

import (
	"context"
	"hash"

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

type parallelSinkEmitter struct {
	ctx context.Context

	client    SinkClient
	batchCfg  sinkBatchConfig
	retryOpts retry.Options
	successCh chan int
	errorCh   chan error

	topicNamer *TopicNamer

	workerCh   []chan rowPayload
	numWorkers int64
	hasher     hash.Hash32

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

func (pse *parallelSinkEmitter) Flush(ctx context.Context) error {
	// Tell each topic producer to flush
	for _, workerCh := range pse.workerCh {
		select {
		case <-pse.ctx.Done():
			return pse.ctx.Err()
		case <-pse.doneCh:
			return nil
		case workerCh <- rowPayload{shouldFlush: true}:
			return nil
		}
	}
	return nil
}

func (pse *parallelSinkEmitter) Close() error {
	close(pse.doneCh)
	return pse.wg.Wait()
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
	metrics metricsRecorder,
) AsyncSink {
	pse := &parallelSinkEmitter{
		ctx:       ctx,
		client:    client,
		batchCfg:  config,
		retryOpts: retryOpts,
		// TODO: TopicNamer

		successCh: make(chan int, 256),
		errorCh:   make(chan error, 1),

		workerCh:   make([]chan rowPayload, numWorkers),
		numWorkers: numWorkers,
		hasher:     makeHasher(),

		wg:      ctxgroup.WithContext(ctx),
		doneCh:  make(chan struct{}),
		metrics: metrics,
	}

	for worker := int64(0); worker < pse.numWorkers; worker++ {
		workerCh := make(chan rowPayload, 256)
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
	payload := rowPayload{
		msg: messagePayload{
			key: key,
			val: value,
		},
		mvcc:  mvcc,
		alloc: alloc,
		topic: topic,
	}
	workerId := pse.workerIndex(payload)
	select {
	case <-pse.ctx.Done():
		return pse.ctx.Err()
	case <-pse.doneCh:
		return nil
	case pse.workerCh[workerId] <- payload:
		return nil
	}
}

func (pse *parallelSinkEmitter) workerLoop(input chan rowPayload) error {
	emitter := makeBatchedSinkEmitter(
		pse.ctx,
		pse.client,
		pse.batchCfg,
		pse.retryOpts,
		pse.successCh,
		pse.errorCh,
		timeutil.DefaultTimeSource{},
		pse.metrics,
	)
	for {
		select {
		case <-pse.ctx.Done():
			return pse.ctx.Err()
		case <-pse.doneCh:
			return nil
		case row := <-input:
			emitter.Emit(row)
		}
	}
}

func (pse *parallelSinkEmitter) workerIndex(row rowPayload) int64 {
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
		err = emitWithRetries(pse.ctx, payload, 1, pse.client, pse.retryOpts, pse.metrics)
		if err != nil {
			return err
		}
	}
	return nil
}
