package changefeedccl

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type FlushingEmitter interface {
	Emit(*sinkEvent)
	// Flush is expected to never be called while Emit() is able to be invoked
	Flush() error
	Close() error
}

type flushingSinkEmitter struct {
	ctx     context.Context
	wrapped AsyncSinkEmitter

	inFlight int64
	flushCh  chan struct{}
	termCh   chan struct{}
	mu       struct {
		syncutil.RWMutex
		shouldNotify bool
		termErr      error
	}

	doneCh chan struct{}
	g      ctxgroup.Group
}

var _ FlushingEmitter = (*flushingSinkEmitter)(nil)

func (fs *flushingSinkEmitter) Emit(event *sinkEvent) {
	fs.incInFlight()
	fs.wrapped.Emit(event)
}

func makeFlushingEmitter(ctx context.Context, wrapped AsyncSinkEmitter) FlushingEmitter {
	sink := flushingSinkEmitter{
		ctx:      ctx,
		wrapped:  wrapped,
		inFlight: 0,
		flushCh:  make(chan struct{}, 1),
		termCh:   make(chan struct{}, 1),
		doneCh:   make(chan struct{}),
		g:        ctxgroup.WithContext(ctx),
	}
	sink.g.GoCtx(func(ctx2 context.Context) error {
		return sink.emitConfirmationWorker(ctx2)
	})

	return &sink
}

func (fs *flushingSinkEmitter) Close() error {
	_ = fs.wrapped.Close()
	close(fs.doneCh)
	_ = fs.g.Wait()
	return nil
}

func (fs *flushingSinkEmitter) incInFlight() {
	atomic.AddInt64(&fs.inFlight, 1)
}

func (fs *flushingSinkEmitter) decInFlight(flushed int) {
	fs.mu.RLock()
	remaining := atomic.AddInt64(&fs.inFlight, -int64(flushed))
	notifyFlush := remaining == 0 && fs.mu.shouldNotify
	fs.mu.RUnlock()
	// If shouldNotify is true, it is assumed that no new Emits could happen,
	// therefore it is not possible for this to occur multiple times for a single
	// Flush call
	if notifyFlush {
		fs.flushCh <- struct{}{}
	}
}

func (fs *flushingSinkEmitter) Flush() error {
	fs.mu.Lock()
	if fs.inFlight == 0 {
		fs.mu.Unlock()
		return nil
	}
	fs.mu.shouldNotify = true
	fs.mu.Unlock()

	fs.wrapped.Emit(newSinkFlushEvent())

	select {
	case <-fs.ctx.Done():
		return fs.ctx.Err()
	case <-fs.doneCh:
		return nil
	case <-fs.termCh:
		fs.mu.RLock()
		defer fs.mu.RUnlock()
		return fs.mu.termErr
	case <-fs.flushCh:
		fs.mu.Lock()
		defer fs.mu.Unlock()
		fs.mu.shouldNotify = false
		return nil
	}
}

func (fs *flushingSinkEmitter) emitConfirmationWorker(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-fs.termCh:
			fs.mu.RLock()
			defer fs.mu.RUnlock()
			return fs.mu.termErr
		case <-fs.doneCh:
			return nil
		case err := <-fs.wrapped.Errors():
			fs.mu.Lock()
			if fs.mu.termErr == nil {
				fs.mu.termErr = err
				close(fs.termCh)
			}
			fs.mu.Unlock()
		case flushed := <-fs.wrapped.Successes():
			fs.decInFlight(flushed)
		}
	}
}
