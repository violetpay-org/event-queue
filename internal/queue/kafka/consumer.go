package kafkaqueue

import (
	"github.com/IBM/sarama"
	"log"
)

type Consumer struct {
	callback func(*sarama.ConsumerMessage)
}

func NewConsumer(callback func(message *sarama.ConsumerMessage)) *Consumer {
	return &Consumer{
		callback: callback,
	}
}

// ConsumeClaim must start a Consumer loop of ConsumerGroupClaim's Messages().
// NOTE: This must not be called within a goroutine, already handled as goroutines by sarama.
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			c.callback(msg)

		case <-session.Context().Done():
			return nil
		}
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (c *Consumer) Setup(session sarama.ConsumerGroupSession) error {
	log.Print("Consumer up and running")
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (c *Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Print("Consumer stopped")
	return nil
}

func (c *Consumer) Callback() func(*sarama.ConsumerMessage) {
	return c.callback
}

type MarkableConsumer struct {
	callback func(*sarama.ConsumerMessage, func())
}

func NewMarkableConsumer(callback func(message *sarama.ConsumerMessage, ack func())) *MarkableConsumer {
	return &MarkableConsumer{
		callback: callback,
	}
}

// ConsumeClaim must start a Consumer loop of ConsumerGroupClaim's Messages().
// NOTE: This must not be called within a goroutine, already handled as goroutines by sarama.
func (c *MarkableConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				// 메세지 채널 닫혔을 때
				return nil
			}

			c.callback(msg, func() {
				session.MarkMessage(msg, "")
			})

		case <-session.Context().Done():
			return nil
		}
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (c *MarkableConsumer) Setup(session sarama.ConsumerGroupSession) error {
	log.Print("Consumer up and running")
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (c *MarkableConsumer) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Print("Consumer stopped")
	return nil
}

func (c *MarkableConsumer) Callback() func(*sarama.ConsumerMessage, func()) {
	return c.callback
}
