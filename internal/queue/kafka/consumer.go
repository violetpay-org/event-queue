package kafkaqueue

import "github.com/IBM/sarama"

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
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (c *Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}
