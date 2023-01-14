package changefeedccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// SinkClient is the interface to an external sink, where batches of messages
// can be encoded into a formatted payload that can be emitted to the sink, and
// these payloads can be emitted.  Emitting is a separate step to Encoding to
// allow for the same encoded payload to retry the Emitting.
type SinkClient interface {
	EncodeBatch([]messagePayload) (SinkPayload, error)
	EncodeResolvedMessage(resolvedMessagePayload) (SinkPayload, error)
	EmitPayload(topic string, payload SinkPayload) error
	Close() error
}

// SinkPayload is an interface representing a sink-specific representation of a
// batch of messages that is ready to be emitted by its EmitRow method.
type SinkPayload interface{}

// messagePayload represents a KV event to be emitted.
type messagePayload struct {
	key   []byte
	val   []byte
	topic string
}

// resolvedMessagePayload represents a Resolved event to be emitted.
type resolvedMessagePayload struct {
	body       []byte
	resolvedTs hlc.Timestamp
	topic      string
}

func emitWithRetries(
	ctx context.Context,
	topic string,
	payload SinkPayload,
	numMessages int,
	emitter SinkClient,
	opts retry.Options,
	metrics metricsRecorder,
) error {
	initialSend := true
	return retry.WithMaxAttempts(ctx, opts, opts.MaxRetries+1, func() error {
		if !initialSend {
			metrics.recordInternalRetry(int64(numMessages), false)
		}
		err := emitter.EmitPayload(topic, payload)
		initialSend = false
		return err
	})
}
