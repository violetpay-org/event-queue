package kafkaqueue

import (
	"context"
	"errors"

	"github.com/IBM/sarama"
	"github.com/violetpay-org/event-queue/queue"
)

type KafkaConsumerGroupOperator[Msg any] struct {
	serializer queue.MessageSerializer[*sarama.ConsumerMessage, Msg]
	callback   queue.Callback[Msg]

	brokers []string
	topic   string
	groupId string
	config  *sarama.Config

	// Only for internal use
	initialized          bool
	cancel               *context.CancelFunc
	consumerGroupHandler sarama.ConsumerGroupHandler
	consumerGroup        sarama.ConsumerGroup
}

func NewConsumeOperator[Msg any](
	serializer queue.MessageSerializer[*sarama.ConsumerMessage, Msg],
	callback queue.Callback[Msg],
	brokers []string,
	topic string,
	groupId string,
	config *sarama.Config,
) queue.Consumer[*sarama.ConsumerMessage, Msg] {
	consumerOperator := KafkaConsumerGroupOperator[Msg]{
		serializer: serializer,
		callback:   callback,
		brokers:    brokers,
		topic:      topic,
		groupId:    groupId,
		config:     config,
	}

	if err := consumerOperator.init(); err != nil {
		// 생성시 발생하는 에러는 panic으로 처리합니다.
		// 이 경우, 오직서서버가 시작할때 발생하는 에러이기 때문에
		// panic으로 처리해도 큰 문제가 없습니다.
		panic(err)
	}

	return &consumerOperator
}

func (k *KafkaConsumerGroupOperator[Msg]) QueueName() string {
	return k.topic
}

func (k *KafkaConsumerGroupOperator[Msg]) Serializer() queue.MessageSerializer[*sarama.ConsumerMessage, Msg] {
	return k.serializer
}

func (k *KafkaConsumerGroupOperator[Msg]) Callback() queue.Callback[Msg] {
	return k.callback
}

func (k *KafkaConsumerGroupOperator[Msg]) init() error {
	var consumerGroupHandler sarama.ConsumerGroupHandler

	if k.config.Consumer.Offsets.AutoCommit.Enable {
		consumerGroupHandler = NewConsumerGroupHandlerForAutoCommit(k.consumeRawMsg)
	} else {
		consumerGroupHandler = NewConsumerGroupHandlerForManualCommit(k.consumeRawMsg)
	}

	k.consumerGroupHandler = consumerGroupHandler
	consumerGroup, err := sarama.NewConsumerGroup(
		k.brokers,
		k.groupId,
		k.config,
	)
	if err != nil {
		return err
	}

	k.consumerGroup = consumerGroup
	k.initialized = true
	return nil
}

func (k *KafkaConsumerGroupOperator[Msg]) consumeRawMsg(msg *sarama.ConsumerMessage) error {
	serializedMsg, err := k.serializer.Serialize(msg)
	if err != nil {
		return err
	}

	return k.callback(serializedMsg)
}

func (k *KafkaConsumerGroupOperator[Msg]) StopConsume() error {
	if k.cancel == nil {
		return errors.New("tried to stop consume before starting (or operator no started, but StopConsume called)")
	}

	(*k.cancel)()

	return nil
}

func (k *KafkaConsumerGroupOperator[Msg]) StartConsume() error {
	if !k.initialized {
		return queue.ErrStartConsume
	}

	ctx, cancel := context.WithCancel(context.Background())
	k.cancel = &cancel

	go k.keepConsumptionAlive(ctx)

	return nil
}

func (k *KafkaConsumerGroupOperator[Msg]) keepConsumptionAlive(ctx context.Context) {
	topics := []string{k.topic}
	isConsumerClosed := func(err error) bool {
		if err == nil {
			return false
		}
		return errors.Is(err, sarama.ErrClosedConsumerGroup)
	}

consumptionLoop:
	for {
		if err := k.consumerGroup.Consume(
			ctx,
			topics,
			k.consumerGroupHandler,
		); isConsumerClosed(err) {
			break consumptionLoop
		}

		if ctx.Err() != nil {
			break consumptionLoop
		}
	}

	return
}

type BeforeBytesProduceOperator struct {
	pool *ProducerPool

	brokers        []string
	configProvider func() *sarama.Config
}

func NewBytesProduceOperatorCtor(brokers []string, configProvider func() *sarama.Config) *BeforeBytesProduceOperator {
	return &BeforeBytesProduceOperator{
		pool:           NewProducerPool(brokers, configProvider),
		brokers:        brokers,
		configProvider: configProvider,
	}
}

func (k *BeforeBytesProduceOperator) Dest(topic string) queue.ProduceOperator[[]byte] {
	return NewBytesProduceOperator(k.pool, k.brokers, topic, k.configProvider)
}

type BytesProduceOperator struct {
	pool *ProducerPool

	brokers        []string
	configProvider func() *sarama.Config
	topic          string
}

func NewBytesProduceOperator(pool *ProducerPool, brokers []string, topic string, configProvider func() *sarama.Config) *BytesProduceOperator {
	return &BytesProduceOperator{
		brokers:        brokers,
		topic:          topic,
		configProvider: configProvider,
		pool:           pool,
	}
}

func (k *BytesProduceOperator) Produce(message []byte) error {
	producer := k.pool.Take()
	defer k.pool.Return(producer)

	_, _, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: k.brokers[0],
		Value: sarama.ByteEncoder(message),
	})

	return err
}
