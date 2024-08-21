// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafkaexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter"

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/kafka"
)

var errUnrecognizedEncoding = fmt.Errorf("unrecognized encoding")

type ProducerHook interface {
	component.Component
	Ack(ctx context.Context, topic string, partition, offset int64)
}

type DeferConsumerBuilder interface {
	component.Component
	BuildLogConsumer() consumer.ConsumeLogsFunc
	//BuildMetricsConsumer() consumer.ConsumeMetricsFunc
	//BuildTracesConsumer() consumer.ConsumeTracesFunc
}

type deferConsumerBuilderHolder struct {
	f consumer.ConsumeLogsFunc
}

func (d *deferConsumerBuilderHolder) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	return d.f(ctx, ld)
}

// kafkaTracesProducer uses sarama to produce trace messages to Kafka.
type kafkaTracesProducer struct {
	cfg       Config
	producer  sarama.SyncProducer
	topic     string
	marshaler TracesMarshaler
	logger    *zap.Logger
}

type kafkaErrors struct {
	count int
	err   string
}

func (ke kafkaErrors) Error() string {
	return fmt.Sprintf("Failed to deliver %d messages due to %s", ke.count, ke.err)
}

func (e *kafkaTracesProducer) tracesPusher(ctx context.Context, td ptrace.Traces) error {
	messages, err := e.marshaler.Marshal(td, e.topic)
	if err != nil {
		return consumererror.NewPermanent(err)
	}
	err = e.producer.SendMessages(messages)
	if err != nil {
		var prodErr sarama.ProducerErrors
		if errors.As(err, &prodErr) {
			if len(prodErr) > 0 {
				return kafkaErrors{len(prodErr), prodErr[0].Err.Error()}
			}
		}
		return err
	}
	return nil
}

func (e *kafkaTracesProducer) Close(ctx context.Context) error {
	if e.producer == nil {
		return nil
	}
	return e.producer.Close()
}

func (e *kafkaTracesProducer) start(ctx context.Context, _ component.Host) error {
	producer, err := newSaramaProducer(e.cfg)
	if err != nil {
		return err
	}
	e.producer = producer
	return nil
}

// kafkaMetricsProducer uses sarama to produce metrics messages to kafka
type kafkaMetricsProducer struct {
	cfg       Config
	producer  sarama.SyncProducer
	topic     string
	marshaler MetricsMarshaler
	logger    *zap.Logger
}

func (e *kafkaMetricsProducer) metricsDataPusher(ctx context.Context, md pmetric.Metrics) error {
	messages, err := e.marshaler.Marshal(md, e.topic)
	if err != nil {
		return consumererror.NewPermanent(err)
	}
	err = e.producer.SendMessages(messages)
	if err != nil {
		var prodErr sarama.ProducerErrors
		if errors.As(err, &prodErr) {
			if len(prodErr) > 0 {
				return kafkaErrors{len(prodErr), prodErr[0].Err.Error()}
			}
		}
		return err
	}
	return nil
}

func (e *kafkaMetricsProducer) Close(ctx context.Context) error {
	if e.producer == nil {
		return nil
	}
	return e.producer.Close()
}

func (e *kafkaMetricsProducer) start(ctx context.Context, _ component.Host) error {
	producer, err := newSaramaProducer(e.cfg)
	if err != nil {
		return err
	}
	e.producer = producer
	return nil
}

// kafkaLogsProducer uses sarama to produce logs messages to kafka
type kafkaLogsProducer struct {
	cfg       Config
	producer  sarama.SyncProducer
	topic     string
	marshaler LogsMarshaler
	logger    *zap.Logger
	hook      ProducerHook
}

func (e *kafkaLogsProducer) logsDataPusher(ctx context.Context, ld plog.Logs) error {
	messages, err := e.marshaler.Marshal(ld, e.topic)
	if err != nil {
		return consumererror.NewPermanent(err)
	}
	err = e.producer.SendMessages(messages)
	if err != nil {
		var prodErr sarama.ProducerErrors
		if errors.As(err, &prodErr) {
			if len(prodErr) > 0 {
				return kafkaErrors{len(prodErr), prodErr[0].Err.Error()}
			}
		}
		return err
	}
	if e.hook != nil {
		logs := ld.ResourceLogs()
		for i := 0; i < logs.Len(); i++ {
			attributes := logs.At(i).Resource().Attributes()
			var (
				topic             string
				partition, offset int64
			)

			if t, ok := attributes.Get("topic"); ok {
				topic = t.Str()
			}
			if p, ok := attributes.Get("partition"); ok {
				partition = p.Int()
			}
			if o, ok := attributes.Get("offset"); ok {
				offset = o.Int()
			}
			e.hook.Ack(ctx, topic, partition, offset)
		}
	}

	return nil
}

func (e *kafkaLogsProducer) Close(ctx context.Context) error {
	if e.producer == nil {
		return nil
	}
	defer func(hook ProducerHook, ctx context.Context) {
		if hook != nil {
			_ = hook.Shutdown(ctx)
		}
	}(e.hook, ctx)
	return e.producer.Close()
}

func (e *kafkaLogsProducer) start(ctx context.Context, host component.Host) error {
	producer, err := newSaramaProducer(e.cfg)
	if err != nil {
		return err
	}
	e.producer = producer
	return e.hook.Start(ctx, host)
}

func newSaramaProducer(config Config) (sarama.SyncProducer, error) {
	c := sarama.NewConfig()

	c.ClientID = config.ClientID

	// These setting are required by the sarama.SyncProducer implementation.
	c.Producer.Return.Successes = true
	c.Producer.Return.Errors = true
	c.Producer.RequiredAcks = config.Producer.RequiredAcks
	// Because sarama does not accept a Context for every message, set the Timeout here.
	c.Producer.Timeout = config.Timeout
	c.Metadata.Full = config.Metadata.Full
	c.Metadata.Retry.Max = config.Metadata.Retry.Max
	c.Metadata.Retry.Backoff = config.Metadata.Retry.Backoff
	c.Producer.MaxMessageBytes = config.Producer.MaxMessageBytes
	c.Producer.Flush.MaxMessages = config.Producer.FlushMaxMessages
	c.Producer.Flush.Bytes = 512 * 1024
	c.Producer.Flush.Frequency = 100 * time.Millisecond
	c.Producer.Flush.Messages = 1024

	if config.ResolveCanonicalBootstrapServersOnly {
		c.Net.ResolveCanonicalBootstrapServers = true
	}

	if config.ProtocolVersion != "" {
		version, err := sarama.ParseKafkaVersion(config.ProtocolVersion)
		if err != nil {
			return nil, err
		}
		c.Version = version
	}

	if err := kafka.ConfigureAuthentication(config.Authentication, c); err != nil {
		return nil, err
	}

	compression, err := saramaProducerCompressionCodec(config.Producer.Compression)
	if err != nil {
		return nil, err
	}
	c.Producer.Compression = compression

	producer, err := sarama.NewSyncProducer(config.Brokers, c)
	if err != nil {
		return nil, err
	}
	return producer, nil
}

func newMetricsExporter(config Config, set exporter.CreateSettings, marshalers map[string]MetricsMarshaler) (*kafkaMetricsProducer, error) {
	marshaler := marshalers[config.Encoding]
	if marshaler == nil {
		return nil, errUnrecognizedEncoding
	}
	return &kafkaMetricsProducer{
		cfg:       config,
		topic:     config.Topic,
		marshaler: marshaler,
		logger:    set.Logger,
	}, nil

}

// newTracesExporter creates Kafka exporter.
func newTracesExporter(config Config, set exporter.CreateSettings, marshalers map[string]TracesMarshaler) (*kafkaTracesProducer, error) {
	marshaler := marshalers[config.Encoding]
	if marshaler == nil {
		return nil, errUnrecognizedEncoding
	}
	if config.PartitionTracesByID {
		if keyableMarshaler, ok := marshaler.(KeyableTracesMarshaler); ok {
			keyableMarshaler.Key()
		}
	}

	return &kafkaTracesProducer{
		cfg:       config,
		topic:     config.Topic,
		marshaler: marshaler,
		logger:    set.Logger,
	}, nil
}

func newLogsExporter(config Config, set exporter.CreateSettings, marshalers map[string]LogsMarshaler, hook ProducerHook) (*kafkaLogsProducer, error) {
	marshaler := marshalers[config.Encoding]
	if marshaler == nil {
		return nil, errUnrecognizedEncoding
	}

	return &kafkaLogsProducer{
		cfg:       config,
		topic:     config.Topic,
		marshaler: marshaler,
		logger:    set.Logger,
		hook:      hook,
	}, nil

}
