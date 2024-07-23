package kafkaqueue

import (
	"context"
	"errors"
	"github.com/IBM/sarama"
	"github.com/violetpay-org/event-queue/queue"
)

type ConsumeOperator[Msg any] struct {
	serializer queue.MessageSerializer[*sarama.ConsumerMessage, Msg]
	callback   queue.Callback[Msg]

	initialized bool
	cancel      *context.CancelFunc

	consumer *Consumer
	brokers  []string
	topic    string
	groupId  string
	config   *sarama.Config
}

func NewConsumeOperator[Msg any](serializer queue.MessageSerializer[*sarama.ConsumerMessage, Msg], callback queue.Callback[Msg], brokers []string, topic string, groupId string, config *sarama.Config) *ConsumeOperator[Msg] {
	return &ConsumeOperator[Msg]{
		serializer: serializer,
		callback:   callback,
		brokers:    brokers,
		topic:      topic,
		groupId:    groupId,
		config:     config,
	}
}

func (k *ConsumeOperator[Msg]) QueueName() string {
	return k.topic
}

func (k *ConsumeOperator[Msg]) Serializer() queue.MessageSerializer[*sarama.ConsumerMessage, Msg] {
	return k.serializer
}

func (k *ConsumeOperator[Msg]) Callback() queue.Callback[Msg] {
	return k.callback
}

func (k *ConsumeOperator[Msg]) Consume(msg *sarama.ConsumerMessage) {
	sMsg, err := k.serializer.Serialize(msg)
	if err != nil {
		return
	}

	k.callback(sMsg)
}

func (k *ConsumeOperator[Msg]) Init() {
	consumer := NewConsumer(k.Consume)

	k.consumer = consumer
	k.initialized = true
}

func (k *ConsumeOperator[Msg]) StartConsume() error {
	if !k.initialized {
		k.Init()
	}

	client, err := sarama.NewConsumerGroup(k.brokers, k.groupId, k.config)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	k.cancel = &cancel

	go func() {
		for {
			if err := client.Consume(ctx, []string{k.topic}, k.consumer); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
			}

			if ctx.Err() != nil {
				return
			}
		}
	}()

	return nil
}

func (k *ConsumeOperator[Msg]) StopConsume() error {
	if k.cancel == nil {
		return errors.New("tried to stop consume before starting (or operator no started, but StopConsume called)")
	}

	(*k.cancel)()

	return nil
}

type BytesProduceOperatorCtor struct {
	pool *ProducerPool

	brokers        []string
	configProvider func() *sarama.Config
}

func NewBytesProduceOperatorCtor(brokers []string, configProvider func() *sarama.Config) *BytesProduceOperatorCtor {
	return &BytesProduceOperatorCtor{
		pool:           NewProducerPool(brokers, configProvider),
		brokers:        brokers,
		configProvider: configProvider,
	}
}

func (k *BytesProduceOperatorCtor) Dest(topic string) queue.ProduceOperator[[]byte] {
	return newBytesProduceOperatorFromLowLevel(k.pool, k.brokers, topic, k.configProvider)
}

type BytesProduceOperator struct {
	pool *ProducerPool

	brokers        []string
	configProvider func() *sarama.Config
	topic          string
}

func newBytesProduceOperatorFromLowLevel(pool *ProducerPool, brokers []string, topic string, configProvider func() *sarama.Config) *BytesProduceOperator {
	return &BytesProduceOperator{
		brokers:        brokers,
		topic:          topic,
		configProvider: configProvider,
		pool:           pool,
	}
}

func NewBytesProduceOperator(brokers []string, topic string, configProvider func() *sarama.Config) *BytesProduceOperator {
	return &BytesProduceOperator{
		pool:           NewProducerPool(brokers, configProvider),
		brokers:        brokers,
		configProvider: configProvider,
		topic:          topic,
	}
}

func (k *BytesProduceOperator) QueueName() string {
	return k.topic
}

func (k *BytesProduceOperator) Produce(message []byte) error {
	producer := k.pool.Take()
	defer k.pool.Return(producer)

	if producer == nil {
		return errors.New("internal error, producer is nil")
	}

	if message == nil {
		return errors.New("message is nil")
	}

	producer.Input() <- &sarama.ProducerMessage{
		Topic: k.brokers[0],
		Value: sarama.ByteEncoder(message),
	}

	return nil
}
