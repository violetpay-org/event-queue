package kafkaqueue

import (
	"github.com/IBM/sarama"
	"github.com/violetpay-org/event-queue/queue"
)

type ConsumerCallback = queue.Callback[*sarama.ConsumerMessage]

type ConsumerGroupHandlerWithCallback interface {
	sarama.ConsumerGroupHandler
	Callback() ConsumerCallback
}

func NewConsumerGroupHandlerForAutoCommit(callback ConsumerCallback) ConsumerGroupHandlerWithCallback {
	return &ConsumerGroupHandlerForAutoCommit{
		callback: callback,
	}
}

func NewConsumerGroupHandlerForManualCommit(callback ConsumerCallback) ConsumerGroupHandlerWithCallback {
	return &ConsumerGroupHandlerForManualCommit{
		callback: callback,
	}
}
