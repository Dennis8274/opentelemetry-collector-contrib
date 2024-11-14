package kafkareceiver

import (
	"github.com/IBM/sarama"
	"go.opentelemetry.io/collector/pdata/plog"
	"golang.org/x/net/context"
)

type CustomExtractor interface {
	Name() string
	ExtractLogs(context.Context, plog.Logs, *sarama.ConsumerMessage)
}

type noCustomExtractor struct {
}

func (n *noCustomExtractor) Name() string {
	return ""
}

func (n *noCustomExtractor) ExtractLogs(context.Context, plog.Logs, *sarama.ConsumerMessage) {
}
