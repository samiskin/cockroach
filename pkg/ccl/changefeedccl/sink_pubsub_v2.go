package changefeedccl

import (
	"context"
	"net/url"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type pubsubEmitter struct {
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

var _ SinkClient = (*pubsubEmitter)(nil)

func (pe *pubsubEmitter) EncodeBatch(msgs []messagePayload) (SinkPayload, error) {
	sinkMessages := make([]pubsubMessagePayload, 0, len(msgs))
	// for _, msg := range msgs {
	// 	var content []byte
	// 	var err error
	// 	topicName, err := pe.topicNamer.Name(msg.topic)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	switch pe.format {
	// 	case changefeedbase.OptFormatJSON:
	// 		content, err = json.Marshal(jsonPayload{
	// 			Key:   msg.key,
	// 			Value: msg.val,
	// 			Topic: topicName,
	// 		})
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 	case changefeedbase.OptFormatCSV:
	// 		content = msg.val
	// 	}
	//
	// 	sinkMessages = append(sinkMessages, pubsubMessagePayload{
	// 		content: content,
	// 		topic:   topicName,
	// 	})
	// }
	return pubsubPayload{messages: sinkMessages}, nil
}

func (pe *pubsubEmitter) EncodeResolvedMessage(
	payload resolvedMessagePayload,
) (SinkPayload, error) {
	sinkMessages := make([]pubsubMessagePayload, 0)
	if err := pe.topicNamer.Each(func(topic string) error {
		sinkMessages = append(sinkMessages, pubsubMessagePayload{
			content: payload.body,
			topic:   topic,
		})
		return nil
	}); err != nil {
		return nil, err
	}

	return pubsubPayload{messages: sinkMessages}, nil
}

func (pe *pubsubEmitter) getTopicClient(topic string) (*pubsub.Topic, error) {
	tc, ok := pe.topicCache[topic]
	if ok {
		return tc, nil
	}

	tc, err := pe.client.CreateTopic(pe.ctx, topic)
	if err != nil {
		switch status.Code(err) {
		case codes.AlreadyExists:
			tc = pe.client.Topic(topic)
		case codes.PermissionDenied:
			// PermissionDenied may not be fatal if the topic already exists,
			// but record it in case it turns out not to.
			pe.topicCreateErr = err
			tc = pe.client.Topic(topic)
		default:
			pe.topicCreateErr = err
			return nil, err
		}
	}
	tc.PublishSettings.DelayThreshold = 100 * time.Minute
	tc.PublishSettings.CountThreshold = 10000
	tc.PublishSettings.ByteThreshold = 1e12

	return tc, nil
}

func (pe *pubsubEmitter) EmitPayload(payload SinkPayload) error {
	pbPayload, ok := payload.(pubsubPayload)
	if !ok {
		return errors.Errorf("cannot construct pubsub payload from given sinkPayload")
	}

	results := make([]*pubsub.PublishResult, 0)
	topics := make(map[string]*pubsub.Topic)
	for _, msg := range pbPayload.messages {
		topicClient, ok := topics[msg.topic]
		if !ok {
			tc, err := pe.getTopicClient(msg.topic)
			if err != nil {
				return err
			}
			topics[msg.topic] = tc
			topicClient = tc
		}

		res := topicClient.Publish(pe.ctx, &pubsub.Message{
			Data: msg.content,
		})
		results = append(results, res)
	}
	for _, tc := range topics {
		tc.Flush()
	}
	for _, res := range results {
		_, err := res.Get(pe.ctx)
		if status.Code(err) == codes.NotFound && pe.topicCreateErr != nil {
			return errors.WithHint(
				errors.Wrap(pe.topicCreateErr,
					"Topic not found, and attempt to autocreate it failed."),
				"Create topics in advance or grant this service account the pubsub.editor role on your project.")
		} else if err != nil {
			return err
		}
	}
	return nil
}

func (pe *pubsubEmitter) Close() error {
	if err := pe.topicNamer.Each(func(topic string) error {
		t, err := pe.getTopicClient(topic)
		if err != nil {
			return err
		}
		t.Stop()
		return nil
	}); err != nil {
		return err
	}

	return pe.client.Close()
}

type pubsubMessagePayload struct {
	content []byte
	topic   string
}
type pubsubPayload struct {
	messages []pubsubMessagePayload
}

func makePubsubEmitter(
	ctx context.Context,
	u *url.URL,
	encodingOpts changefeedbase.EncodingOptions,
	targets changefeedbase.Targets,
	knobs *TestingKnobs,
) (SinkClient, error) {
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

	pubsubTopicName := pubsubURL.consumeParam(changefeedbase.SinkParamTopicName)
	topicNamer, err := MakeTopicNamer(targets, WithSingleName(pubsubTopicName))
	if err != nil {
		return nil, err
	}

	var client *pubsub.Client
	if knobs != nil && knobs.PubsubClientOverride != nil {
		client, err = knobs.PubsubClientOverride(ctx)
	} else {
		client, err = makeClient(ctx, pubsubURL)
	}
	if err != nil {
		return nil, err
	}

	thinSink := &pubsubEmitter{
		ctx:        ctx,
		client:     client,
		topicNamer: topicNamer,
		format:     formatType,
	}

	return thinSink, nil
}

func makeClient(ctx context.Context, url sinkURL) (*pubsub.Client, error) {
	const regionParam = "region"
	projectID := url.Host
	if projectID == "" {
		return nil, errors.New("missing project name")
	}
	region := url.consumeParam(regionParam)
	if region == "" {
		return nil, errors.New("region query parameter not found")
	}

	creds, err := getGCPCredentials(ctx, url)
	if err != nil {
		return nil, err
	}
	options := []option.ClientOption{
		creds,
		option.WithEndpoint(gcpEndpointForRegion(region)),
	}

	client, err := pubsub.NewClient(
		ctx,
		projectID,
		options...,
	)
	if err != nil {
		return nil, errors.Wrap(err, "opening client")
	}
	return client, nil
}

func makePubsubSink(
	ctx context.Context,
	u *url.URL,
	encodingOpts changefeedbase.EncodingOptions,
	jsonConfig changefeedbase.SinkSpecificJSONConfig,
	targets changefeedbase.Targets,
	source timeutil.TimeSource,
	mb metricsRecorderBuilder,
	knobs *TestingKnobs,
) (Sink, error) {
	return nil, nil
	// thinSink, err := makePubsubEmitter(ctx, u, encodingOpts, targets, knobs)
	// if err != nil {
	// 	return nil, err
	// }
	//
	// flushCfg, retryOpts, err := getWorkerConfigFromJson(jsonConfig)
	// if err != nil {
	// 	return nil, err
	// }
	// flushCfg.Messages = 100
	// return makeBatchingWorkerSink(ctx, thinSink, flushCfg, retryOpts, source, mb)
}
