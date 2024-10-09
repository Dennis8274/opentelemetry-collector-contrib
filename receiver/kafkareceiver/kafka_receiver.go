// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafkareceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kafkareceiver"

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/IBM/sarama"
	"github.com/rcrowley/go-metrics"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/kafka"
)

const (
	transport = "kafka"
)

var errInvalidInitialOffset = fmt.Errorf("invalid initial offset")

const (
	kafkaMarkMessageCallback = 1

	AttrKeyRecvTopic     = "receiver_topic"
	AttrKeyRecvPartition = "receiver_partition"
	AttrKeyRecvOffset    = "receiver_offset"
)

type markMessageCallback func()

type HandlerHook interface {
	sarama.ConsumerGroupHandler
	Init(Config, receiver.CreateSettings)
	Start(context.Context, component.Host) error
	Shutdown(context.Context) error
	Ack(topic string, partition, offset int64)
}

// KafkaTracesConsumer uses sarama to consume and handle messages from kafka.
type KafkaTracesConsumer struct {
	config            Config
	consumerGroup     sarama.ConsumerGroup
	nextConsumer      consumer.Traces
	topics            []string
	cancelConsumeLoop context.CancelFunc
	unmarshaler       TracesUnmarshaler

	settings receiver.CreateSettings

	autocommitEnabled bool
	messageMarking    MessageMarking
	headerExtraction  bool
	headers           []string

	delegate HandlerHook
}

// KafkaMetricsConsumer uses sarama to consume and handle messages from kafka.
type KafkaMetricsConsumer struct {
	config            Config
	consumerGroup     sarama.ConsumerGroup
	nextConsumer      consumer.Metrics
	topics            []string
	cancelConsumeLoop context.CancelFunc
	unmarshaler       MetricsUnmarshaler

	settings receiver.CreateSettings

	autocommitEnabled bool
	messageMarking    MessageMarking
	headerExtraction  bool
	headers           []string

	delegate HandlerHook
}

// KafkaLogsConsumer uses sarama to consume and handle messages from kafka.
type KafkaLogsConsumer struct {
	config            Config
	consumerGroup     sarama.ConsumerGroup
	nextConsumer      consumer.Logs
	topics            []string
	cancelConsumeLoop context.CancelFunc
	unmarshaler       LogsUnmarshaler

	settings receiver.CreateSettings

	autocommitEnabled bool
	messageMarking    MessageMarking
	headerExtraction  bool
	headers           []string

	delegate HandlerHook
}

var _ receiver.Traces = (*KafkaTracesConsumer)(nil)
var _ receiver.Metrics = (*KafkaMetricsConsumer)(nil)
var _ receiver.Logs = (*KafkaLogsConsumer)(nil)

func newTracesReceiver(config Config, set receiver.CreateSettings, unmarshaler TracesUnmarshaler, hook HandlerHook, nextConsumer consumer.Traces) (*KafkaTracesConsumer, error) {
	if unmarshaler == nil {
		return nil, errUnrecognizedEncoding
	}

	hook.Init(config, set)
	return &KafkaTracesConsumer{
		config:            config,
		topics:            []string{config.Topic},
		nextConsumer:      nextConsumer,
		unmarshaler:       unmarshaler,
		settings:          set,
		autocommitEnabled: config.AutoCommit.Enable,
		messageMarking:    config.MessageMarking,
		headerExtraction:  config.HeaderExtraction.ExtractHeaders,
		headers:           config.HeaderExtraction.Headers,
		delegate:          hook,
	}, nil
}

func createKafkaClient(config Config) (sarama.ConsumerGroup, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = config.ClientID
	saramaConfig.Metadata.Full = config.Metadata.Full
	saramaConfig.Metadata.Retry.Max = config.Metadata.Retry.Max
	saramaConfig.Metadata.Retry.Backoff = config.Metadata.Retry.Backoff
	saramaConfig.Consumer.Fetch.Default = 3 * 1024 * 1024
	saramaConfig.ChannelBufferSize = 1024
	saramaConfig.Consumer.Offsets.AutoCommit.Enable = config.AutoCommit.Enable
	saramaConfig.Consumer.Offsets.AutoCommit.Interval = config.AutoCommit.Interval
	saramaConfig.MetricRegistry = metrics.DefaultRegistry
	var err error
	if saramaConfig.Consumer.Offsets.Initial, err = toSaramaInitialOffset(config.InitialOffset); err != nil {
		return nil, err
	}
	if config.ResolveCanonicalBootstrapServersOnly {
		saramaConfig.Net.ResolveCanonicalBootstrapServers = true
	}
	if config.ProtocolVersion != "" {
		if saramaConfig.Version, err = sarama.ParseKafkaVersion(config.ProtocolVersion); err != nil {
			return nil, err
		}
	}
	if err := kafka.ConfigureAuthentication(config.Authentication, saramaConfig); err != nil {
		return nil, err
	}
	return sarama.NewConsumerGroup(config.Brokers, config.GroupID, saramaConfig)
}

func (c *KafkaTracesConsumer) Start(ctx context.Context, host component.Host) error {
	ctx, cancel := context.WithCancel(ctx)
	if c.delegate != nil {
		if err := c.delegate.Start(ctx, host); err != nil {
			cancel()
			return err
		}
	}
	c.cancelConsumeLoop = cancel
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             c.settings.ID,
		Transport:              transport,
		ReceiverCreateSettings: c.settings,
	})
	if err != nil {
		return err
	}
	// consumerGroup may be set in tests to inject fake implementation.
	if c.consumerGroup == nil {
		if c.consumerGroup, err = createKafkaClient(c.config); err != nil {
			return err
		}
	}

	consumerGroup := &TracesConsumerGroupHandler{
		logger:            c.settings.Logger,
		unmarshaler:       c.unmarshaler,
		nextConsumer:      c.nextConsumer,
		ready:             make(chan bool),
		obsrecv:           obsrecv,
		autocommitEnabled: c.autocommitEnabled,
		messageMarking:    c.messageMarking,
		headerExtractor:   &nopHeaderExtractor{},
		delegate:          c.delegate,
	}

	if c.headerExtraction {
		consumerGroup.headerExtractor = &headerExtractor{
			logger:  c.settings.Logger,
			headers: c.headers,
		}
	}
	go func() {
		if err := c.consumeLoop(ctx, consumerGroup); err != nil {
			c.settings.ReportStatus(component.NewFatalErrorEvent(err))
		}
	}()
	<-consumerGroup.ready
	return nil
}

func (c *KafkaTracesConsumer) consumeLoop(ctx context.Context, handler sarama.ConsumerGroupHandler) error {
	for {
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		if err := c.consumerGroup.Consume(ctx, c.topics, handler); err != nil {
			c.settings.Logger.Error("Error from consumer", zap.Error(err))
		}
		// check if context was cancelled, signaling that the consumer should stop
		if ctx.Err() != nil {
			c.settings.Logger.Info("Consumer stopped", zap.Error(ctx.Err()))
			return ctx.Err()
		}
	}
}

func (c *KafkaTracesConsumer) Shutdown(ctx context.Context) error {
	if c.delegate != nil {
		_ = c.delegate.Shutdown(ctx)
	}
	if c.cancelConsumeLoop == nil {
		return nil
	}
	c.cancelConsumeLoop()
	if c.consumerGroup == nil {
		return nil
	}
	return c.consumerGroup.Close()
}

func newMetricsReceiver(config Config, set receiver.CreateSettings, unmarshaler MetricsUnmarshaler, hook HandlerHook, nextConsumer consumer.Metrics) (*KafkaMetricsConsumer, error) {
	if unmarshaler == nil {
		return nil, errUnrecognizedEncoding
	}

	hook.Init(config, set)
	return &KafkaMetricsConsumer{
		config:            config,
		topics:            []string{config.Topic},
		nextConsumer:      nextConsumer,
		unmarshaler:       unmarshaler,
		settings:          set,
		autocommitEnabled: config.AutoCommit.Enable,
		messageMarking:    config.MessageMarking,
		headerExtraction:  config.HeaderExtraction.ExtractHeaders,
		headers:           config.HeaderExtraction.Headers,
		delegate:          hook,
	}, nil
}

func (c *KafkaMetricsConsumer) Start(ctx context.Context, host component.Host) error {
	ctx, cancel := context.WithCancel(ctx)
	if c.delegate != nil {
		if err := c.delegate.Start(ctx, host); err != nil {
			cancel()
			return err
		}
	}
	c.cancelConsumeLoop = cancel
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             c.settings.ID,
		Transport:              transport,
		ReceiverCreateSettings: c.settings,
	})
	if err != nil {
		return err
	}
	// consumerGroup may be set in tests to inject fake implementation.
	if c.consumerGroup == nil {
		if c.consumerGroup, err = createKafkaClient(c.config); err != nil {
			return err
		}
	}

	metricsConsumerGroup := &MetricsConsumerGroupHandler{
		logger:            c.settings.Logger,
		unmarshaler:       c.unmarshaler,
		nextConsumer:      c.nextConsumer,
		ready:             make(chan bool),
		obsrecv:           obsrecv,
		autocommitEnabled: c.autocommitEnabled,
		messageMarking:    c.messageMarking,
		headerExtractor:   &nopHeaderExtractor{},
		delegate:          c.delegate,
	}

	if c.headerExtraction {
		metricsConsumerGroup.headerExtractor = &headerExtractor{
			logger:  c.settings.Logger,
			headers: c.headers,
		}
	}
	go func() {
		if err := c.consumeLoop(ctx, metricsConsumerGroup); err != nil {
			c.settings.ReportStatus(component.NewFatalErrorEvent(err))
		}
	}()
	<-metricsConsumerGroup.ready
	return nil
}

func (c *KafkaMetricsConsumer) consumeLoop(ctx context.Context, handler sarama.ConsumerGroupHandler) error {
	for {
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		if err := c.consumerGroup.Consume(ctx, c.topics, handler); err != nil {
			c.settings.Logger.Error("Error from consumer", zap.Error(err))
		}
		// check if context was cancelled, signaling that the consumer should stop
		if ctx.Err() != nil {
			c.settings.Logger.Info("Consumer stopped", zap.Error(ctx.Err()))
			return ctx.Err()
		}
	}
}

func (c *KafkaMetricsConsumer) Shutdown(context.Context) error {
	if c.cancelConsumeLoop == nil {
		return nil
	}
	c.cancelConsumeLoop()
	if c.consumerGroup == nil {
		return nil
	}
	return c.consumerGroup.Close()
}

func newLogsReceiver(config Config, set receiver.CreateSettings, unmarshaler LogsUnmarshaler, hook HandlerHook, nextConsumer consumer.Logs) (*KafkaLogsConsumer, error) {
	if unmarshaler == nil {
		return nil, errUnrecognizedEncoding
	}

	hook.Init(config, set)
	return &KafkaLogsConsumer{
		config:            config,
		topics:            []string{config.Topic},
		nextConsumer:      nextConsumer,
		unmarshaler:       unmarshaler,
		settings:          set,
		autocommitEnabled: config.AutoCommit.Enable,
		messageMarking:    config.MessageMarking,
		headerExtraction:  config.HeaderExtraction.ExtractHeaders,
		headers:           config.HeaderExtraction.Headers,
		delegate:          hook,
	}, nil
}

func (c *KafkaLogsConsumer) Start(ctx context.Context, host component.Host) error {
	ctx, cancel := context.WithCancel(ctx)
	if c.delegate != nil {
		if err := c.delegate.Start(ctx, host); err != nil {
			cancel()
			return err
		}
	}
	c.cancelConsumeLoop = cancel
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             c.settings.ID,
		Transport:              transport,
		ReceiverCreateSettings: c.settings,
	})
	if err != nil {
		return err
	}
	// consumerGroup may be set in tests to inject fake implementation.
	if c.consumerGroup == nil {
		if c.consumerGroup, err = createKafkaClient(c.config); err != nil {
			return err
		}
	}

	logsConsumerGroup := &LogsConsumerGroupHandler{
		logger:            c.settings.Logger,
		unmarshaler:       c.unmarshaler,
		nextConsumer:      c.nextConsumer,
		ready:             make(chan bool),
		obsrecv:           obsrecv,
		autocommitEnabled: c.autocommitEnabled,
		messageMarking:    c.messageMarking,
		headerExtractor:   &nopHeaderExtractor{},
		delegate:          c.delegate,
	}

	if c.headerExtraction {
		logsConsumerGroup.headerExtractor = &headerExtractor{
			logger:  c.settings.Logger,
			headers: c.headers,
		}
	}
	go func() {
		if err := c.consumeLoop(ctx, logsConsumerGroup); err != nil {
			if !errors.Is(err, context.Canceled) {
				c.settings.ReportStatus(component.NewFatalErrorEvent(err))
			}
		}
	}()
	<-logsConsumerGroup.ready
	return nil
}

func (c *KafkaLogsConsumer) consumeLoop(ctx context.Context, handler sarama.ConsumerGroupHandler) error {
	for {
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		if err := c.consumerGroup.Consume(ctx, c.topics, handler); err != nil {
			c.settings.Logger.Error("Error from consumer", zap.Error(err))
		}
		// check if context was cancelled, signaling that the consumer should stop
		if ctx.Err() != nil {
			c.settings.Logger.Info("Consumer stopped", zap.Error(ctx.Err()))
			return ctx.Err()
		}
	}
}

func (c *KafkaLogsConsumer) Shutdown(context.Context) error {
	if c.cancelConsumeLoop == nil {
		return nil
	}
	c.cancelConsumeLoop()
	if c.consumerGroup == nil {
		return nil
	}
	return c.consumerGroup.Close()
}

type TracesConsumerGroupHandler struct {
	id           component.ID
	unmarshaler  TracesUnmarshaler
	nextConsumer consumer.Traces
	ready        chan bool
	readyCloser  sync.Once

	logger *zap.Logger

	obsrecv *receiverhelper.ObsReport

	autocommitEnabled bool
	messageMarking    MessageMarking
	headerExtractor   HeaderExtractor
	delegate          HandlerHook
}

type MetricsConsumerGroupHandler struct {
	id           component.ID
	unmarshaler  MetricsUnmarshaler
	nextConsumer consumer.Metrics
	ready        chan bool
	readyCloser  sync.Once

	logger *zap.Logger

	obsrecv *receiverhelper.ObsReport

	autocommitEnabled bool
	messageMarking    MessageMarking
	headerExtractor   HeaderExtractor
	delegate          HandlerHook
}

type LogsConsumerGroupHandler struct {
	id           component.ID
	unmarshaler  LogsUnmarshaler
	nextConsumer consumer.Logs
	ready        chan bool
	readyCloser  sync.Once

	logger *zap.Logger

	obsrecv *receiverhelper.ObsReport

	autocommitEnabled bool
	messageMarking    MessageMarking
	headerExtractor   HeaderExtractor
	delegate          HandlerHook
}

var _ sarama.ConsumerGroupHandler = (*TracesConsumerGroupHandler)(nil)
var _ sarama.ConsumerGroupHandler = (*MetricsConsumerGroupHandler)(nil)
var _ sarama.ConsumerGroupHandler = (*LogsConsumerGroupHandler)(nil)

func (c *TracesConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	c.readyCloser.Do(func() {
		close(c.ready)
	})
	statsTags := []tag.Mutator{tag.Upsert(tagInstanceName, c.id.Name())}
	_ = stats.RecordWithTags(session.Context(), statsTags, statPartitionStart.M(1))
	if c.delegate != nil {
		return c.delegate.Setup(session)
	}
	return nil
}

func (c *TracesConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	statsTags := []tag.Mutator{tag.Upsert(tagInstanceName, c.id.Name())}
	_ = stats.RecordWithTags(session.Context(), statsTags, statPartitionClose.M(1))
	if c.delegate != nil {
		return c.delegate.Cleanup(session)
	}
	return nil
}

func (c *TracesConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	c.logger.Info("Starting consumer group", zap.Int32("partition", claim.Partition()))
	if c.delegate != nil {
		if err := c.delegate.ConsumeClaim(session, claim); err != nil {
			return err
		}
	}
	if !c.autocommitEnabled {
		defer session.Commit()
	}

	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			c.logger.Debug("Kafka message claimed",
				zap.String("value", string(message.Value)),
				zap.Time("timestamp", message.Timestamp),
				zap.String("topic", message.Topic))
			if !c.messageMarking.After {
				session.MarkMessage(message, "")
			}

			ctx := c.obsrecv.StartTracesOp(session.Context())
			statsTags := []tag.Mutator{
				tag.Upsert(tagInstanceName, c.id.String()),
				tag.Upsert(tagPartition, strconv.Itoa(int(claim.Partition()))),
			}
			_ = stats.RecordWithTags(ctx, statsTags,
				statMessageCount.M(1),
				statMessageOffset.M(message.Offset),
				statMessageOffsetLag.M(claim.HighWaterMarkOffset()-message.Offset-1))

			traces, err := c.unmarshaler.Unmarshal(message.Value)
			if err != nil {
				c.logger.Error("failed to unmarshal message", zap.Error(err))
				_ = stats.RecordWithTags(
					ctx,
					[]tag.Mutator{tag.Upsert(tagInstanceName, c.id.String())},
					statUnmarshalFailedSpans.M(1))
				if c.messageMarking.After && c.messageMarking.OnError {
					session.MarkMessage(message, "")
				}
				return err
			}

			c.headerExtractor.extractHeadersTraces(traces, message)
			spanCount := traces.SpanCount()
			err = c.nextConsumer.ConsumeTraces(session.Context(), traces)
			c.obsrecv.EndTracesOp(ctx, c.unmarshaler.Encoding(), spanCount, err)
			if err != nil {
				if c.messageMarking.After && c.messageMarking.OnError {
					session.MarkMessage(message, "")
				}
				return err
			}
			if c.messageMarking.After {
				session.MarkMessage(message, "")
			}
			if !c.autocommitEnabled {
				session.Commit()
			}

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}

func (c *MetricsConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	c.readyCloser.Do(func() {
		close(c.ready)
	})
	statsTags := []tag.Mutator{tag.Upsert(tagInstanceName, c.id.Name())}
	_ = stats.RecordWithTags(session.Context(), statsTags, statPartitionStart.M(1))
	if c.delegate != nil {
		return c.delegate.Setup(session)
	}
	return nil
}

func (c *MetricsConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	statsTags := []tag.Mutator{tag.Upsert(tagInstanceName, c.id.Name())}
	_ = stats.RecordWithTags(session.Context(), statsTags, statPartitionClose.M(1))
	if c.delegate != nil {
		return c.delegate.Cleanup(session)
	}
	return nil
}

func (c *MetricsConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	c.logger.Info("Starting consumer group", zap.Int32("partition", claim.Partition()))
	if c.delegate != nil {
		if err := c.delegate.ConsumeClaim(session, claim); err != nil {
			return err
		}
	}
	if !c.autocommitEnabled {
		defer session.Commit()
	}

	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			c.logger.Debug("Kafka message claimed",
				zap.String("value", string(message.Value)),
				zap.Time("timestamp", message.Timestamp),
				zap.String("topic", message.Topic))
			if !c.messageMarking.After {
				session.MarkMessage(message, "")
			}

			ctx := c.obsrecv.StartMetricsOp(session.Context())
			statsTags := []tag.Mutator{
				tag.Upsert(tagInstanceName, c.id.String()),
				tag.Upsert(tagPartition, strconv.Itoa(int(claim.Partition()))),
			}
			_ = stats.RecordWithTags(ctx, statsTags,
				statMessageCount.M(1),
				statMessageOffset.M(message.Offset),
				statMessageOffsetLag.M(claim.HighWaterMarkOffset()-message.Offset-1))

			metrics, err := c.unmarshaler.Unmarshal(message.Value)
			if err != nil {
				c.logger.Error("failed to unmarshal message", zap.Error(err))
				_ = stats.RecordWithTags(
					ctx,
					[]tag.Mutator{tag.Upsert(tagInstanceName, c.id.String())},
					statUnmarshalFailedMetricPoints.M(1))
				if c.messageMarking.After && c.messageMarking.OnError {
					session.MarkMessage(message, "")
				}
				return err
			}

			topic := message.Topic
			partition := int64(message.Partition)
			offset := message.Offset
			resourceMetrics := metrics.ResourceMetrics()
			for i := 0; i < resourceMetrics.Len(); i++ {
				attributes := resourceMetrics.At(i).Resource().Attributes()
				attributes.PutStr(AttrKeyRecvTopic, topic)
				attributes.PutInt(AttrKeyRecvPartition, partition)
				attributes.PutInt(AttrKeyRecvOffset, offset)
			}

			c.headerExtractor.extractHeadersMetrics(metrics, message)

			dataPointCount := metrics.DataPointCount()
			err = c.nextConsumer.ConsumeMetrics(context.WithValue(session.Context(), kafkaMarkMessageCallback, markMessageCallback(func() {
				c.delegate.Ack(topic, partition, offset)
			})), metrics)
			c.obsrecv.EndMetricsOp(ctx, c.unmarshaler.Encoding(), dataPointCount, err)
			if err != nil {
				if c.messageMarking.After && c.messageMarking.OnError {
					session.MarkMessage(message, "")
				}
				return err
			}
			if c.messageMarking.After {
				session.MarkMessage(message, "")
			}
			if !c.autocommitEnabled {
				session.Commit()
			}

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}

func (c *LogsConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	c.readyCloser.Do(func() {
		close(c.ready)
	})
	_ = stats.RecordWithTags(
		session.Context(),
		[]tag.Mutator{tag.Upsert(tagInstanceName, c.id.String())},
		statPartitionStart.M(1))
	if c.delegate != nil {
		return c.delegate.Setup(session)
	}
	return nil
}

func (c *LogsConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	_ = stats.RecordWithTags(
		session.Context(),
		[]tag.Mutator{tag.Upsert(tagInstanceName, c.id.String())},
		statPartitionClose.M(1))
	if c.delegate != nil {
		return c.delegate.Cleanup(session)
	}
	return nil
}

func (c *LogsConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	c.logger.Info("Starting consumer group", zap.Int32("partition", claim.Partition()))
	if c.delegate != nil {
		if err := c.delegate.ConsumeClaim(session, claim); err != nil {
			return err
		}
	}
	if !c.autocommitEnabled {
		defer session.Commit()
	}

	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			c.logger.Debug("Kafka message claimed",
				zap.String("value", string(message.Value)),
				zap.Time("timestamp", message.Timestamp),
				zap.String("topic", message.Topic))
			if !c.messageMarking.After {
				session.MarkMessage(message, "")
			}

			ctx := c.obsrecv.StartLogsOp(session.Context())
			statsTags := []tag.Mutator{
				tag.Upsert(tagInstanceName, c.id.String()),
				tag.Upsert(tagTopic, claim.Topic()),
				tag.Upsert(tagPartition, strconv.Itoa(int(claim.Partition()))),
			}
			_ = stats.RecordWithTags(
				ctx,
				statsTags,
				statMessageCount.M(1),
				statMessageOffset.M(message.Offset),
				statMessageOffsetLag.M(claim.HighWaterMarkOffset()-message.Offset-1))

			logs, err := c.unmarshaler.Unmarshal(message.Value)
			if err != nil {
				c.logger.Error("failed to unmarshal message", zap.Error(err))
				_ = stats.RecordWithTags(
					ctx,
					[]tag.Mutator{tag.Upsert(tagInstanceName, c.id.String())},
					statUnmarshalFailedLogRecords.M(1))
				if c.messageMarking.After && c.messageMarking.OnError {
					session.MarkMessage(message, "")
				}
				return err
			}
			c.headerExtractor.extractHeadersLogs(logs, message)

			topic := message.Topic
			partition := int64(message.Partition)
			offset := message.Offset
			resourceLogs := logs.ResourceLogs()
			for i := 0; i < resourceLogs.Len(); i++ {
				attributes := resourceLogs.At(i).Resource().Attributes()
				attributes.PutStr(AttrKeyRecvTopic, topic)
				attributes.PutInt(AttrKeyRecvPartition, partition)
				attributes.PutInt(AttrKeyRecvOffset, offset)
			}
			logRecordCount := logs.LogRecordCount()
			err = c.nextConsumer.ConsumeLogs(context.WithValue(session.Context(), kafkaMarkMessageCallback, markMessageCallback(func() {
				c.delegate.Ack(topic, partition, offset)
			})), logs)
			c.obsrecv.EndLogsOp(ctx, c.unmarshaler.Encoding(), logRecordCount, err)
			if err != nil {
				if c.messageMarking.After && c.messageMarking.OnError {
					session.MarkMessage(message, "")
				}
				return err
			}
			if c.messageMarking.After {
				session.MarkMessage(message, "")
			}
			if !c.autocommitEnabled {
				session.Commit()
			}

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}

func MarkMessage(ctx context.Context) {
	value := ctx.Value(kafkaMarkMessageCallback)
	if value == nil {
		return
	}
	callback, ok := value.(markMessageCallback)
	if !ok {
		return
	}
	callback()
}

func toSaramaInitialOffset(initialOffset string) (int64, error) {
	switch initialOffset {
	case offsetEarliest:
		return sarama.OffsetOldest, nil
	case offsetLatest:
		fallthrough
	case "":
		return sarama.OffsetNewest, nil
	default:
		return 0, errInvalidInitialOffset
	}
}
