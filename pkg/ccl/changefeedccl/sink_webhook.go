// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type SinkWorkerFactory interface {
	MakeEventSink() (SinkWorker, error)
}

type sinkWorkerFactory struct {
	builder func() (SinkWorker, error)
}

var _ SinkWorkerFactory = (*sinkWorkerFactory)(nil)

func (swf *sinkWorkerFactory) MakeEventSink() (SinkWorker, error) {
	return swf.builder()
}

// SinkPayload is an interface representing a sink-specific representation of a
// batch of messages that is ready to be emitted by its EmitRow method.
type SinkPayload interface{}

// MessagePayload represents a KV event to be emitted.
type MessagePayload struct {
	key []byte
	val []byte
}

// ResolvedMessagePayload represents a Resolved event to be emitted.
type ResolvedMessagePayload struct {
	body       []byte
	resolvedTs hlc.Timestamp
}

type messageBatch struct {
	buffer      []MessagePayload
	bufferBytes int
	alloc       kvevent.Alloc
	mvcc        hlc.Timestamp
	bufferTime  time.Time // The earliest time a message was inserted into the batch
}

func newMessageBatch() messageBatch {
	return messageBatch{
		buffer:      make([]MessagePayload, 0),
		bufferBytes: 0,
	}
}

func (mb *messageBatch) isEmpty() bool {
	return len(mb.buffer) == 0
}

func (mb *messageBatch) reset() {
	mb.buffer = mb.buffer[:0]
	mb.bufferBytes = 0
	mb.alloc = kvevent.Alloc{}
}

type rowPayload struct {
	msg   MessagePayload
	alloc kvevent.Alloc
	mvcc  hlc.Timestamp
}

func (mb *messageBatch) moveIntoBuffer(p rowPayload, keyInValue bool) {
	if mb.isEmpty() {
		mb.bufferTime = timeutil.Now()
	}

	mb.buffer = append(mb.buffer, p.msg)
	mb.bufferBytes += len(p.msg.val)

	// Don't double-count the key bytes if the key is included in the value
	if !keyInValue {
		mb.bufferBytes += len(p.msg.key)
	}

	if mb.mvcc.IsEmpty() || p.mvcc.Less(mb.mvcc) {
		mb.mvcc = p.mvcc
	}

	mb.alloc.Merge(&p.alloc)
}

type batchWorkerMessage struct {
	sinkPayload SinkPayload
	numMessages int
	mvcc        hlc.Timestamp
	alloc       kvevent.Alloc
	kvBytes     int
	bufferTime  time.Time
}

type SinkWorker interface {
	Sink
	Successes() chan int
	Errors() chan error
}

type batchingWorkerSink struct {
	ctx context.Context

	ts         ThinSink
	config     batchConfig
	retryOpts  retry.Options
	timeSource timeutil.TimeSource

	successCh      chan int
	errorCh        chan error
	triggerFlushCh chan struct{}

	doneCh chan struct{}

	batchCh     chan batchWorkerMessage
	rowCh       chan rowPayload
	workerGroup ctxgroup.Group

	metrics metricsRecorder
}

var _ SinkWorker = (*batchingWorkerSink)(nil) // sinkProcessor should implement Sink

// Flush is a no-op to be compliant with Sink, users are expected to rely on
// Successes() and Errors()
func (bs *batchingWorkerSink) Flush(ctx context.Context) error {
	return nil
}

func (bs *batchingWorkerSink) Dial() error {
	return nil
}

func (bs *batchingWorkerSink) Successes() chan int {
	return bs.successCh
}

func (bs *batchingWorkerSink) Errors() chan error {
	return bs.errorCh
}

func makeBatchingWorkerSink(
	ctx context.Context, sink ThinSink, config batchConfig, retryOpts retry.Options, timeSource timeutil.TimeSource, mb metricsRecorderBuilder,
) (SinkWorker, error) {
	bs := batchingWorkerSink{
		ctx:       ctx,
		ts:        sink,
		batchCh:   make(chan batchWorkerMessage, 256),
		rowCh:     make(chan rowPayload, 256),
		successCh: make(chan int, 256),
		errorCh:   make(chan error, 256),

		triggerFlushCh: make(chan struct{}),
		doneCh:         make(chan struct{}),
		workerGroup:    ctxgroup.WithContext(ctx),
		timeSource:     timeSource,
		config:         config,
		retryOpts:      retryOpts,
		metrics:        mb(requiresResourceAccounting),
	}

	// Since flushes need to be triggerable from both EmitRow and a timer firing,
	// they must be done in a dedicated goroutine.
	bs.workerGroup.GoCtx(func(ctx context.Context) error {
		return bs.startBatchWorker()
	})

	// Since messages should be able to be batched while a batch is in-flight to
	// the downstream sink, emitting is done in its own goroutine.
	bs.workerGroup.GoCtx(func(ctx context.Context) error {
		return bs.startEmitWorker()
	})

	return &bs, nil
}

func (bs *batchingWorkerSink) Close() error {
	close(bs.doneCh)
	// Ignore errors related to outstanding messages since we're either shutting
	// down or beginning to retry regardless
	_ = bs.workerGroup.Wait()
	return nil
}

func (bs *batchingWorkerSink) handleError(err error) {
	bs.errorCh <- changefeedbase.MarkRetryableError(err)
}

func (bs *batchingWorkerSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	bs.rowCh <- rowPayload{
		msg: MessagePayload{
			key: key,
			val: value,
		},
		mvcc:  mvcc,
		alloc: alloc,
	}
	return nil
}

func (bs *batchingWorkerSink) startBatchWorker() error {
	currentBatch := newMessageBatch()

	flushBatch := func() {
		if currentBatch.isEmpty() {
			return
		}

		// Reuse the same batch to avoid need for garbage collection
		defer currentBatch.reset()

		// Process messages into a payload ready to be emitted to the sink
		sinkPayload, err := bs.ts.EncodeBatch(currentBatch.buffer)
		if err != nil {
			bs.handleError(err)
			return
		}

		// Send the encoded batch to a separate worker so that flushes do not block
		// further message aggregation
		bs.batchCh <- batchWorkerMessage{
			sinkPayload: sinkPayload,
			alloc:       currentBatch.alloc,
			numMessages: len(currentBatch.buffer),
			mvcc:        currentBatch.mvcc,
			kvBytes:     currentBatch.bufferBytes,
			bufferTime:  currentBatch.bufferTime,
		}
	}

	flushTimer := bs.timeSource.NewTimer()

	for {
		select {
		case <-bs.ctx.Done():
			return bs.ctx.Err()
		case <-bs.doneCh:
			return nil
		case rowMsg := <-bs.rowCh:
			bs.metrics.recordMessageSize(int64(len(rowMsg.msg.key) + len(rowMsg.msg.val)))

			// If the batch is about to no longer be empty, start the flush timer
			if currentBatch.isEmpty() && time.Duration(bs.config.Frequency) > 0 {
				flushTimer.Reset(time.Duration(bs.config.Frequency))
			}
			currentBatch.moveIntoBuffer(rowMsg, false) // TODO: Key in value

			if bs.shouldFlushBatch(currentBatch) {
				bs.metrics.recordSizeBasedFlush()
				flushBatch()
			}
		case <-bs.triggerFlushCh:
			flushBatch()
		case <-flushTimer.Ch():
			flushTimer.MarkRead()
			flushBatch()
		}
	}
}

func (bs *batchingWorkerSink) shouldFlushBatch(batch messageBatch) bool {
	switch {
	// all zero values is interpreted as flush every time
	case bs.config.Messages == 0 && bs.config.Bytes == 0 && bs.config.Frequency == 0:
		return true
	// messages threshold has been reached
	case bs.config.Messages > 0 && len(batch.buffer) >= bs.config.Messages:
		return true
	// bytes threshold has been reached
	case bs.config.Bytes > 0 && batch.bufferBytes >= bs.config.Bytes:
		return true
	default:
		return false
	}
}

func (bs *batchingWorkerSink) startEmitWorker() error {
	for {
		select {
		case <-bs.ctx.Done():
			return bs.ctx.Err()
		case <-bs.doneCh:
			return nil
		case batch := <-bs.batchCh:
			defer bs.metrics.recordFlushRequestCallback()()
			err := bs.sendWithRetries(batch.sinkPayload, batch.numMessages)
			if err != nil {
				bs.handleError(err)
				return nil
			}

			bs.metrics.recordEmittedBatch(
				batch.bufferTime, batch.numMessages, batch.mvcc, batch.kvBytes, sinkDoesNotCompress)
			batch.alloc.Release(bs.ctx)

			bs.successCh <- batch.numMessages
		}
	}
}

func (bs *batchingWorkerSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	defer bs.metrics.recordResolvedCallback()()

	data, err := encoder.EncodeResolvedTimestamp(ctx, "", resolved)
	if err != nil {
		return err
	}
	payload, err := bs.ts.EncodeResolvedMessage(ResolvedMessagePayload{
		resolvedTs: resolved,
		body:       data,
	})
	if err != nil {
		return err
	}
	return bs.sendWithRetries(payload, 1)
}

func (bs *batchingWorkerSink) sendWithRetries(payload SinkPayload, numMessages int) error {
	initialSend := true
	return retry.WithMaxAttempts(bs.ctx, bs.retryOpts, bs.retryOpts.MaxRetries+1, func() error {
		if !initialSend {
			bs.metrics.recordInternalRetry(int64(numMessages), false)
		}
		err := bs.ts.EmitPayload(payload)
		initialSend = false
		return err
	})
}

//------------------------------------------------------

// ThinSink is the interface to an external sink used by a sink processor.
// Messages go through two steps, an Encoding step which creates a payload of
// one or more messages, then an Emit stage which sends the payload, with
// EmitPayload potentially being retried multiple times with the same payload if
// errors are observed.
type ThinSink interface {
	EncodeBatch([]MessagePayload) (SinkPayload, error)
	EncodeResolvedMessage(ResolvedMessagePayload) (SinkPayload, error)
	EmitPayload(SinkPayload) error
	Close() error
}

const (
	applicationTypeJSON = `application/json`
	applicationTypeCSV  = `text/csv`
	authorizationHeader = `Authorization`
)

func isWebhookSink(u *url.URL) bool {
	switch u.Scheme {
	// allow HTTP here but throw an error later to make it clear HTTPS is required
	case changefeedbase.SinkSchemeWebhookHTTP, changefeedbase.SinkSchemeWebhookHTTPS:
		return true
	default:
		return false
	}
}

// The webhook sink uses sinkProcessor with its SinkPayload being HTTP requests
func makeWebhookSink(
	ctx context.Context,
	u sinkURL,
	encodingOpts changefeedbase.EncodingOptions,
	opts changefeedbase.WebhookSinkOptions,
	parallelism int,
	source timeutil.TimeSource,
	mb metricsRecorderBuilder,
) (SinkWorker, error) {
	thinSink, err := makeWebhookThinSink(ctx, u, encodingOpts, opts)
	if err != nil {
		return nil, err
	}

	flushCfg, retryOpts, err := getWebhookSinkConfig(opts.JSONConfig)
	if err != nil {
		return nil, err
	}
	return makeBatchingWorkerSink(ctx, thinSink, flushCfg, retryOpts, source, mb)
}

type webhookThinSink struct {
	ctx        context.Context
	format     changefeedbase.FormatType
	url        sinkURL
	authHeader string
	client     *httputil.Client
}

var _ ThinSink = (*webhookThinSink)(nil)

func makeWebhookThinSink(
	ctx context.Context,
	u sinkURL,
	encodingOpts changefeedbase.EncodingOptions,
	opts changefeedbase.WebhookSinkOptions,
) (ThinSink, error) {
	err := validateWebhookOpts(u, encodingOpts, opts)
	if err != nil {
		return nil, err
	}

	u.Scheme = strings.TrimPrefix(u.Scheme, `webhook-`)

	sink := &webhookThinSink{
		ctx:        ctx,
		authHeader: opts.AuthHeader,
		format:     encodingOpts.Format,
	}

	var connTimeout time.Duration
	if opts.ClientTimeout != nil {
		connTimeout = *opts.ClientTimeout
	}
	sink.client, err = makeWebhookClient(u, connTimeout)
	if err != nil {
		return nil, err
	}

	// remove known query params from sink URL before setting in sink config
	sinkURLParsed, err := url.Parse(u.String())
	if err != nil {
		return nil, err
	}
	params := sinkURLParsed.Query()
	params.Del(changefeedbase.SinkParamSkipTLSVerify)
	params.Del(changefeedbase.SinkParamCACert)
	params.Del(changefeedbase.SinkParamClientCert)
	params.Del(changefeedbase.SinkParamClientKey)
	sinkURLParsed.RawQuery = params.Encode()
	sink.url = sinkURL{URL: sinkURLParsed}

	return sink, nil
}

func makeWebhookClient(u sinkURL, timeout time.Duration) (*httputil.Client, error) {
	client := &httputil.Client{
		Client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				DialContext:     (&net.Dialer{Timeout: timeout}).DialContext,
				IdleConnTimeout: 90 * time.Second, // taken from DefaultTransport

				// Raising this value to 200 makes little difference while reducing it
				// to 50 results in an ~8% reduction in throughput.
				MaxIdleConnsPerHost: 100,
			},
		},
	}

	dialConfig := struct {
		tlsSkipVerify bool
		caCert        []byte
		clientCert    []byte
		clientKey     []byte
	}{}

	transport := client.Transport.(*http.Transport)

	if _, err := u.consumeBool(changefeedbase.SinkParamSkipTLSVerify, &dialConfig.tlsSkipVerify); err != nil {
		return nil, err
	}
	if err := u.decodeBase64(changefeedbase.SinkParamCACert, &dialConfig.caCert); err != nil {
		return nil, err
	}
	if err := u.decodeBase64(changefeedbase.SinkParamClientCert, &dialConfig.clientCert); err != nil {
		return nil, err
	}
	if err := u.decodeBase64(changefeedbase.SinkParamClientKey, &dialConfig.clientKey); err != nil {
		return nil, err
	}

	transport.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: dialConfig.tlsSkipVerify,
	}

	if dialConfig.caCert != nil {
		caCertPool, err := x509.SystemCertPool()
		if err != nil {
			return nil, errors.Wrap(err, "could not load system root CA pool")
		}
		if caCertPool == nil {
			caCertPool = x509.NewCertPool()
		}
		if !caCertPool.AppendCertsFromPEM(dialConfig.caCert) {
			return nil, errors.Errorf("failed to parse certificate data:%s", string(dialConfig.caCert))
		}
		transport.TLSClientConfig.RootCAs = caCertPool
	}

	if dialConfig.clientCert != nil && dialConfig.clientKey == nil {
		return nil, errors.Errorf(`%s requires %s to be set`, changefeedbase.SinkParamClientCert, changefeedbase.SinkParamClientKey)
	} else if dialConfig.clientKey != nil && dialConfig.clientCert == nil {
		return nil, errors.Errorf(`%s requires %s to be set`, changefeedbase.SinkParamClientKey, changefeedbase.SinkParamClientCert)
	}

	if dialConfig.clientCert != nil && dialConfig.clientKey != nil {
		cert, err := tls.X509KeyPair(dialConfig.clientCert, dialConfig.clientKey)
		if err != nil {
			return nil, errors.Wrap(err, `invalid client certificate data provided`)
		}
		transport.TLSClientConfig.Certificates = []tls.Certificate{cert}
	}

	return client, nil
}

func (ws *webhookThinSink) makePayloadForBytes(body []byte) (SinkPayload, error) {
	req, err := http.NewRequestWithContext(ws.ctx, http.MethodPost, ws.url.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	switch ws.format {
	case changefeedbase.OptFormatJSON:
		req.Header.Set("Content-Type", applicationTypeJSON)
	case changefeedbase.OptFormatCSV:
		req.Header.Set("Content-Type", applicationTypeCSV)
	}

	if ws.authHeader != "" {
		req.Header.Set(authorizationHeader, ws.authHeader)
	}

	return req, nil
}

func (ws *webhookThinSink) EncodeBatch(batch []MessagePayload) (SinkPayload, error) {
	var reqBody []byte
	var err error

	switch ws.format {
	case changefeedbase.OptFormatJSON:
		reqBody, err = encodeWebhookMsgJSON(batch)
	case changefeedbase.OptFormatCSV:
		reqBody, err = encodeWebhookMsgCSV(batch)
	}

	if err != nil {
		return nil, err
	}

	return ws.makePayloadForBytes(reqBody)
}

type webhookJsonEvent struct {
	Payload []json.RawMessage `json:"payload"`
	Length  int               `json:"length"`
}

func encodeWebhookMsgJSON(messages []MessagePayload) ([]byte, error) {
	payload := make([]json.RawMessage, len(messages))
	for i, m := range messages {
		payload[i] = m.val
	}

	body := &webhookJsonEvent{
		Payload: payload,
		Length:  len(payload),
	}
	j, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return j, err
}

func encodeWebhookMsgCSV(messages []MessagePayload) ([]byte, error) {
	var mergedMsgs []byte
	for _, m := range messages {
		mergedMsgs = append(mergedMsgs, m.val...)
	}
	return mergedMsgs, nil
}

func (ws *webhookThinSink) EncodeResolvedMessage(
	payload ResolvedMessagePayload,
) (SinkPayload, error) {
	return ws.makePayloadForBytes(payload.body)
}

func (ws *webhookThinSink) EmitPayload(batch SinkPayload) error {
	req := batch.(*http.Request)
	res, err := ws.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if !(res.StatusCode >= http.StatusOK && res.StatusCode < http.StatusMultipleChoices) {
		resBody, err := io.ReadAll(res.Body)
		if err != nil {
			return errors.Wrapf(err, "failed to read body for HTTP response with status: %d", res.StatusCode)
		}
		return fmt.Errorf("%s: %s", res.Status, string(resBody))
	}
	return nil
}

func (ws *webhookThinSink) Close() error {
	ws.client.CloseIdleConnections()
	return nil
}

func getWebhookSinkConfig(
	jsonStr changefeedbase.SinkSpecificJSONConfig,
) (batchCfg batchConfig, retryCfg retry.Options, err error) {
	retryCfg = defaultRetryConfig()

	var cfg webhookSinkConfig
	cfg.Retry.Max = jsonMaxRetries(retryCfg.MaxRetries)
	cfg.Retry.Backoff = jsonDuration(retryCfg.InitialBackoff)
	if jsonStr != `` {
		// set retry defaults to be overridden if included in JSON
		if err = json.Unmarshal([]byte(jsonStr), &cfg); err != nil {
			return batchCfg, retryCfg, errors.Wrapf(err, "error unmarshalling json")
		}
	}

	// don't support negative values
	if cfg.Flush.Messages < 0 || cfg.Flush.Bytes < 0 || cfg.Flush.Frequency < 0 ||
		cfg.Retry.Max < 0 || cfg.Retry.Backoff < 0 {
		return batchCfg, retryCfg, errors.Errorf("invalid option value %s, all config values must be non-negative", changefeedbase.OptWebhookSinkConfig)
	}

	// errors if other batch values are set, but frequency is not
	if (cfg.Flush.Messages > 0 || cfg.Flush.Bytes > 0) && cfg.Flush.Frequency == 0 {
		return batchCfg, retryCfg, errors.Errorf("invalid option value %s, flush frequency is not set, messages may never be sent", changefeedbase.OptWebhookSinkConfig)
	}

	retryCfg.MaxRetries = int(cfg.Retry.Max)
	retryCfg.InitialBackoff = time.Duration(cfg.Retry.Backoff)
	return cfg.Flush, retryCfg, nil
}

func validateWebhookOpts(
	u sinkURL, encodingOpts changefeedbase.EncodingOptions, opts changefeedbase.WebhookSinkOptions,
) error {
	if u.Scheme != changefeedbase.SinkSchemeWebhookHTTPS {
		return errors.Errorf(`this sink requires %s`, changefeedbase.SinkSchemeHTTPS)
	}

	switch encodingOpts.Format {
	case changefeedbase.OptFormatJSON:
	case changefeedbase.OptFormatCSV:
	default:
		return errors.Errorf(`this sink is incompatible with %s=%s`,
			changefeedbase.OptFormat, encodingOpts.Format)
	}

	switch encodingOpts.Envelope {
	case changefeedbase.OptEnvelopeWrapped, changefeedbase.OptEnvelopeBare:
	default:
		return errors.Errorf(`this sink is incompatible with %s=%s`,
			changefeedbase.OptEnvelope, encodingOpts.Envelope)
	}

	if encodingOpts.Envelope != changefeedbase.OptEnvelopeBare && !encodingOpts.KeyInValue {
		return errors.Errorf(`this sink requires the WITH %s option`, changefeedbase.OptKeyInValue)
	}

	if !encodingOpts.TopicInValue {
		return errors.Errorf(`this sink requires the WITH %s option`, changefeedbase.OptTopicInValue)
	}

	return nil
}

type batchConfig struct {
	Bytes, Messages int          `json:",omitempty"`
	Frequency       jsonDuration `json:",omitempty"`
}

type jsonMaxRetries int

func (j *jsonMaxRetries) UnmarshalJSON(b []byte) error {
	var i int64
	// try to parse as int
	i, err := strconv.ParseInt(string(b), 10, 64)
	if err == nil {
		if i <= 0 {
			return errors.Errorf("max retry count must be a positive integer. use 'inf' for infinite retries.")
		}
		*j = jsonMaxRetries(i)
	} else {
		// if that fails, try to parse as string (only accept 'inf')
		var s string
		// using unmarshal here to remove quotes around the string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		if strings.ToLower(s) == "inf" {
			// if used wants infinite retries, set to zero as retry.Options interprets this as infinity
			*j = 0
		} else if n, err := strconv.Atoi(s); err == nil { // also accept ints as strings
			*j = jsonMaxRetries(n)
		} else {
			return errors.Errorf("max retries must be either a positive int or 'inf' for infinite retries.")
		}
	}
	return nil
}

// wrapper structs to unmarshal json, retry.Options will be the actual config
type retryConfig struct {
	Max     jsonMaxRetries `json:",omitempty"`
	Backoff jsonDuration   `json:",omitempty"`
}

// proper JSON schema for webhook sink config:
//
//	{
//	  "Flush": {
//		   "Messages":  ...,
//		   "Bytes":     ...,
//		   "Frequency": ...,
//	  },
//		 "Retry": {
//		   "Max":     ...,
//		   "Backoff": ...,
//	  }
//	}
type webhookSinkConfig struct {
	Flush batchConfig `json:",omitempty"`
	Retry retryConfig `json:",omitempty"`
}

func defaultRetryConfig() retry.Options {
	opts := retry.Options{
		InitialBackoff: 500 * time.Millisecond,
		MaxRetries:     3,
		Multiplier:     2,
	}
	// max backoff should be initial * 2 ^ maxRetries
	opts.MaxBackoff = opts.InitialBackoff * time.Duration(int(math.Pow(2.0, float64(opts.MaxRetries))))
	return opts
}
