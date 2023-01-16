package changefeedccl

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	pubsub "cloud.google.com/go/pubsub/apiv1"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/api/option"
	pb "google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type pubsubSinkClient struct {
	ctx        context.Context
	client     *pubsub.PublisherClient
	topicNamer *TopicNamer
	projectID  string
	format     changefeedbase.FormatType
	mu         struct {
		syncutil.RWMutex
		topicCache map[string]struct{}
	}

	// Topic creation errors may not be an actual issue unless the Publish call
	// itself fails, so creation errors are stored for future use in the event of
	// a publish error.
	topicCreateErr error
}

var _ SinkClient = (*pubsubSinkClient)(nil)

func (pe *pubsubSinkClient) EncodeBatch(msgs []messagePayload) (SinkPayload, error) {
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
		})
	}
	return pubsubPayload{messages: sinkMessages}, nil
}

func (pe *pubsubSinkClient) EncodeResolvedMessage(
	payload resolvedMessagePayload,
) (SinkPayload, error) {
	sinkMessages := make([]pubsubMessagePayload, 0)
	if err := pe.topicNamer.Each(func(topic string) error {
		sinkMessages = append(sinkMessages, pubsubMessagePayload{
			content: payload.body,
		})
		return nil
	}); err != nil {
		return nil, err
	}

	return pubsubPayload{messages: sinkMessages}, nil
}

func (pe *pubsubSinkClient) gcPubsubTopic(topic string) string {
	return fmt.Sprintf("projects/%s/topics/%s", pe.projectID, topic)
}

func (pe *pubsubSinkClient) maybeMakeTopic(topic string) error {
	pe.mu.RLock()
	_, ok := pe.mu.topicCache[topic]
	if ok {
		pe.mu.RUnlock()
		return nil
	}
	pe.mu.RUnlock()
	pe.mu.Lock()
	defer pe.mu.Unlock()
	_, ok = pe.mu.topicCache[topic]
	if ok {
		return nil
	}

	_, err := pe.client.CreateTopic(pe.ctx, &pb.Topic{Name: pe.gcPubsubTopic(topic)})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		if status.Code(err) == codes.PermissionDenied {
			// PermissionDenied may not be fatal if the topic already exists,
			// but record it in case it turns out not to.
			pe.topicCreateErr = err
		} else {
			pe.topicCreateErr = err
			return err
		}
	}
	pe.mu.topicCache[topic] = struct{}{}
	return nil
}

func (pe *pubsubSinkClient) EmitPayload(topic string, payload SinkPayload) error {
	pbPayload, ok := payload.(pubsubPayload)
	if !ok {
		return errors.Errorf("cannot construct pubsub payload from given sinkPayload")
	}

	err := pe.maybeMakeTopic(topic)
	if err != nil {
		return err
	}

	pbMsgs := make([]*pb.PubsubMessage, len(pbPayload.messages))
	for i, msg := range pbPayload.messages {
		// fmt.Printf("\n\x1b[32m EMIT PAYLOAD %s \x1b[0m\n", string(msg.content))
		pbMsgs[i] = &pb.PubsubMessage{
			Data:        msg.content,
			OrderingKey: "",
		}
	}

	_, err = pe.client.Publish(pe.ctx, &pb.PublishRequest{
		Topic:    pe.gcPubsubTopic(topic),
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

func (pe *pubsubSinkClient) Close() error {
	return pe.client.Close()
}

type pubsubMessagePayload struct {
	content []byte
}
type pubsubPayload struct {
	messages []pubsubMessagePayload
}

func makePubsubSinkClient(
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

	projectID := pubsubURL.Host
	if projectID == "" {
		return nil, errors.New("missing project name")
	}

	publisherClient, err := makePublisherClient(ctx, pubsubURL, knobs)
	if err != nil {
		return nil, err
	}

	sinkClient := &pubsubSinkClient{
		ctx:        ctx,
		topicNamer: topicNamer,
		format:     formatType,
		client:     publisherClient,
		projectID:  projectID,
	}
	sinkClient.mu.topicCache = make(map[string]struct{})

	return sinkClient, nil
}

func makePublisherClient(
	ctx context.Context, url sinkURL, knobs *TestingKnobs,
) (*pubsub.PublisherClient, error) {
	const regionParam = "region"
	region := url.consumeParam(regionParam)
	if region == "" {
		return nil, errors.New("region query parameter not found")
	}

	options := []option.ClientOption{
		option.WithEndpoint(gcpEndpointForRegion(region)),
	}

	if knobs == nil || !knobs.PubsubClientSkipCredentialsCheck {
		creds, err := getGCPCredentials(ctx, url)
		if err != nil {
			return nil, err
		}
		options = append(options, creds)
	}

	client, err := pubsub.NewPublisherClient(
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
	sinkClient, err := makePubsubSinkClient(ctx, u, encodingOpts, targets, knobs)
	if err != nil {
		return nil, err
	}

	flushCfg, retryOpts, err := getSinkConfigFromJson(jsonConfig, sinkJSONConfig{
		// GCPubsub defaults
		Flush: sinkBatchConfig{
			Frequency: jsonDuration(10 * time.Millisecond),
			Messages:  100,
			Bytes:     1e6,
		},
	})
	if err != nil {
		return nil, err
	}

	pubsubURL := sinkURL{URL: u, q: u.Query()}
	pubsubTopicName := pubsubURL.consumeParam(changefeedbase.SinkParamTopicName)
	topicNamer, err := MakeTopicNamer(targets, WithSingleName(pubsubTopicName))
	if err != nil {
		return nil, err
	}

	return makeParallelBatchingSink(
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
