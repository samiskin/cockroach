package changefeedccl

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"cloud.google.com/go/pubsub"
	pubsubv1 "cloud.google.com/go/pubsub/apiv1"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/api/option"
	pb "google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type pubsubEmitter struct {
	ctx           context.Context
	client        *pubsub.Client
	publishClient *pubsubv1.PublisherClient
	topicNamer    *TopicNamer
	format        changefeedbase.FormatType
	topicCache    map[string]*pubsub.Topic

	// Topic creation errors may not be an actual issue unless the Publish call
	// itself fails, so creation errors are stored for future use in the event of
	// a publish error.
	topicCreateErr error
}

var _ SinkClient = (*pubsubEmitter)(nil)

func (pe *pubsubEmitter) EncodeBatch(msgs []messagePayload) (SinkPayload, error) {
	sinkMessages := make([]pubsubMessagePayload, 0, len(msgs))
	for _, msg := range msgs {
		var content []byte
		var err error
		switch pe.format {
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
	// tc.PublishSettings.DelayThreshold = 100 * time.Minute
	// tc.PublishSettings.CountThreshold = 10000
	// tc.PublishSettings.ByteThreshold = 1e12

	return tc, nil
}

func (pe *pubsubEmitter) EmitPayload(payload SinkPayload) error {
	pbPayload, ok := payload.(pubsubPayload)
	if !ok {
		return errors.Errorf("cannot construct pubsub payload from given sinkPayload")
	}

	// _, err := pe.getTopicClient(pbPayload.messages[0].topic)
	_, err := pe.publishClient.CreateTopic(pe.ctx, &pb.Topic{Name: pbPayload.messages[0].topic})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return err
	}

	pbMsgs := make([]*pb.PubsubMessage, len(pbPayload.messages))
	for i, msg := range pbPayload.messages {
		pbMsgs[i] = &pb.PubsubMessage{
			Data:        msg.content,
			OrderingKey: "",
		}
	}

	_, err = pe.publishClient.Publish(pe.ctx, &pb.PublishRequest{
		Topic:    pbPayload.messages[0].topic,
		Messages: pbMsgs,
	})

	if status.Code(err) == codes.NotFound && pe.topicCreateErr != nil {
		return errors.WithHint(
			errors.Wrap(pe.topicCreateErr,
				"Topic not found, and attempt to autocreate it failed."),
			"Create topics in advance or grant this service account the pubsub.editor role on your project.")
	} else if err != nil {
		return err
	}
	return nil
}

func (pe *pubsubEmitter) Close() error {
	if err := pe.topicNamer.Each(func(topic string) error {
		fmt.Printf("\n\x1b[34m CLOSE TOPIC %s  \x1b[0m\n", topic)
		t, err := pe.getTopicClient(topic)
		if err != nil {
			return err
		}
		t.Stop()
		return nil
	}); err != nil {
		return err
	}

	fmt.Printf("\n\x1b[34m CLOSE CLIENT  \x1b[0m\n")
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
	publisherClient, err := makePublisherClient(ctx, pubsubURL, knobs)
	if err != nil {
		return nil, err
	}

	thinSink := &pubsubEmitter{
		ctx:           ctx,
		client:        client,
		topicNamer:    topicNamer,
		format:        formatType,
		publishClient: publisherClient,
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

func makePublisherClient(ctx context.Context, url sinkURL, knobs *TestingKnobs) (*pubsubv1.PublisherClient, error) {
	const regionParam = "region"
	var options []option.ClientOption
	if knobs != nil && knobs.PubsubClientOptionsOverride != nil {
		options = knobs.PubsubClientOptionsOverride(ctx)
	} else {
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
		options = []option.ClientOption{
			creds,
			option.WithEndpoint(gcpEndpointForRegion(region)),
		}
	}

	client, err := pubsubv1.NewPublisherClient(
		ctx,
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
	pacer SinkPacer,
) (Sink, error) {
	sinkClient, err := makePubsubEmitter(ctx, u, encodingOpts, targets, knobs)
	if err != nil {
		return nil, err
	}

	flushCfg, retryOpts, err := getSinkConfigFromJson(jsonConfig)
	if err != nil {
		return nil, err
	}

	pubsubURL := sinkURL{URL: u, q: u.Query()}
	pubsubTopicName := pubsubURL.consumeParam(changefeedbase.SinkParamTopicName)
	topicNamer, err := MakeTopicNamer(targets, WithSingleName(pubsubTopicName))
	if err != nil {
		return nil, err
	}

	return makeParallelSinkEmitter(
		ctx,
		sinkClient,
		flushCfg,
		retryOpts,
		int64(64),
		topicNamer,
		mb(requiresResourceAccounting),
		pacer,
	), nil
}
