package changefeedccl

import (
	"context"
	"encoding/json"
	"net/url"

	"cloud.google.com/go/pubsub"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type pubsubThinSink struct {
	ctx        context.Context
	client     *pubsub.Client
	topicNamer *TopicNamer
	format     changefeedbase.FormatType
	topicCache map[string]*pubsub.Topic

	// Topic creation errors may not be an actual issue unless the Publish call
	// itself fails, so creation errors are stored for future use in the event of
	// a publish error.
	topicCreateErr error
}

var _ ThinSink = (*pubsubThinSink)(nil)

func (ps *pubsubThinSink) EncodeBatch(msgs []MessagePayload) (SinkPayload, error) {
	sinkMessages := make([]pubsubMessagePayload, 0, len(msgs))
	for _, msg := range msgs {
		var content []byte
		var err error
		switch ps.format {
		case changefeedbase.OptFormatJSON:
			content, err = json.Marshal(jsonPayload{
				Key:   msg.key,
				Value: msg.val,
				Topic: msg.topic,
			})
			if err != nil {
				return nil, err
			}
		case changefeedbase.OptFormatCSV:
			content = msg.val
		}

		sinkMessages = append(sinkMessages, pubsubMessagePayload{
			content: content,
			topic:   msg.topic,
		})
	}
	return sinkMessages, nil
}

func (ps *pubsubThinSink) EncodeResolvedMessage(payload ResolvedMessagePayload) (SinkPayload, error) {
	sinkMessages := make([]pubsubMessagePayload, 0)
	ps.topicNamer.Each(func(topic string) error {
		sinkMessages = append(sinkMessages, pubsubMessagePayload{
			content: payload.body,
			topic:   topic,
		})
		return nil
	})

	return sinkMessages, nil
}

func (ps *pubsubThinSink) getTopicClient(topic string) (*pubsub.Topic, error) {
	tc, ok := ps.topicCache[topic]
	if ok {
		return tc, nil
	}

	tc, err := ps.client.CreateTopic(ps.ctx, topic)
	if err != nil {
		switch status.Code(err) {
		case codes.AlreadyExists:
			tc = ps.client.Topic(topic)
		case codes.PermissionDenied:
			// PermissionDenied may not be fatal if the topic already exists,
			// but record it in case it turns out not to.
			ps.topicCreateErr = err
			tc = ps.client.Topic(topic)
		default:
			ps.topicCreateErr = err
			return nil, err
		}
	}

	return tc, nil
}

func (ps *pubsubThinSink) EmitPayload(payload SinkPayload) error {
	pbPayload, ok := payload.(pubsubPayload)
	if !ok {
		return errors.Errorf("cannot construct pubsub payload from given sinkPayload")
	}

	results := make([]*pubsub.PublishResult, 0)
	topics := make(map[string]*pubsub.Topic)
	for _, msg := range pbPayload.messages {
		topicClient, ok := topics[msg.topic]
		if !ok {
			tc, err := ps.getTopicClient(msg.topic)
			if err != nil {
				return err
			}
			topics[msg.topic] = tc
			topicClient = tc
		}

		res := topicClient.Publish(ps.ctx, &pubsub.Message{
			Data: msg.content,
		})
		results = append(results, res)
	}
	for _, tc := range topics {
		tc.Flush()
	}
	for _, res := range results {
		_, err := res.Get(ps.ctx)
		if status.Code(err) == codes.NotFound && ps.topicCreateErr != nil {
			return errors.WithHint(
				errors.Wrap(ps.topicCreateErr,
					"Topic not found, and attempt to autocreate it failed."),
				"Create topics in advance or grant this service account the pubsub.editor role on your project.")
		} else if err != nil {
			return err
		}
	}
	return nil
}

func (ps *pubsubThinSink) Close() error {
	ps.topicNamer.Each(func(topic string) error {
		t, err := ps.getTopicClient(topic)
		if err != nil {
			return err
		}
		t.Stop()
		return nil
	})

	return ps.client.Close()
}

type pubsubMessagePayload struct {
	content []byte
	topic   string
}
type pubsubPayload struct {
	messages []pubsubMessagePayload
}

func makePubsubThinSink(
	ctx context.Context,
	u *url.URL,
	encodingOpts changefeedbase.EncodingOptions,
	targets changefeedbase.Targets,
) (ThinSink, error) {
	if u.Scheme != GcpScheme {
		return nil, errors.Errorf("unknown scheme: %s", u.Scheme)
	}

	var formatType changefeedbase.FormatType
	switch encodingOpts.Format {
	case changefeedbase.OptFormatJSON:
		formatType = changefeedbase.OptFormatJSON
	case changefeedbase.OptFormatCSV:
		formatType = changefeedbase.OptFormatCSV
	default:
		return nil, errors.Errorf(`this sink is incompatible with %s=%s`,
			changefeedbase.OptFormat, encodingOpts.Format)
	}

	switch encodingOpts.Envelope {
	case changefeedbase.OptEnvelopeWrapped, changefeedbase.OptEnvelopeBare:
	default:
		return nil, errors.Errorf(`this sink is incompatible with %s=%s`,
			changefeedbase.OptEnvelope, encodingOpts.Envelope)
	}

	pubsubURL := sinkURL{URL: u, q: u.Query()}
	const regionParam = "region"
	projectID := pubsubURL.Host
	if projectID == "" {
		return nil, errors.New("missing project name")
	}
	region := pubsubURL.consumeParam(regionParam)
	if region == "" {
		return nil, errors.New("region query parameter not found")
	}
	pubsubTopicName := pubsubURL.consumeParam(changefeedbase.SinkParamTopicName)
	topicNamer, err := MakeTopicNamer(targets, WithSingleName(pubsubTopicName))
	if err != nil {
		return nil, err
	}

	creds, err := getGCPCredentials(ctx, pubsubURL)
	if err != nil {
		return nil, err
	}
	client, err := pubsub.NewClient(
		ctx,
		projectID,
		creds,
		option.WithEndpoint(gcpEndpointForRegion(region)),
	)
	if err != nil {
		return nil, errors.Wrap(err, "opening client")
	}

	thinSink := &pubsubThinSink{
		ctx:        ctx,
		client:     client,
		topicNamer: topicNamer,
		format:     formatType,
	}

	return thinSink, nil
}

func makePubsubSink(
	ctx context.Context,
	u *url.URL,
	encodingOpts changefeedbase.EncodingOptions,
	jsonConfig changefeedbase.SinkSpecificJSONConfig,
	targets changefeedbase.Targets,
	source timeutil.TimeSource,
	mb metricsRecorderBuilder,
) (Sink, error) {
	thinSink, err := makePubsubThinSink(ctx, u, encodingOpts, targets)
	if err != nil {
		return nil, err
	}

	flushCfg, retryOpts, err := getWorkerConfigFromJson(jsonConfig)
	if err != nil {
		return nil, err
	}
	return makeBatchingWorkerSink(ctx, thinSink, flushCfg, retryOpts, source, mb)
}
