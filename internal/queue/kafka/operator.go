package kafkaqueue

import (
	"context"
	"errors"
	"github.com/IBM/sarama"
	"github.com/violetpay-org/event-queue/queue"
	"log"
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

// AckConsumeOperator 는 Acknowledge (메세지 브로커에게 보내는 메세지 정상 수신 응답) 을 다루는 컨슈머입니다. 실행되는 흐름은 다음과 같습니다.
//
// 1. 내부 컨슈머가 메세지를 받습니다.
//
// 2. 애크 콜백 함수 (AckCallback) 를 호출합니다.
// 만약 AckCallback 함수가 true 를 반환하면, 메세지를 정상적으로 수신했음을 브로커에 알립니다. 이후 다음 과정으로 진행합니다.
// 만약 AckCallback 함수가 false 를 반환하면, 메세지를 수신했음을 브로커에 알리지 않습니다. 이후 다음 과정으로 진행하지 않고 즉시 처리 과정을 종료합니다.
//
// 3. 콜백 함수 (Callback) 를 호출합니다.
//
// 4. 종료합니다.
type AckConsumeOperator[Msg any] struct {
	serializer queue.MessageSerializer[*sarama.ConsumerMessage, Msg]

	ackCallback queue.AckCallback[Msg]
	callback    queue.Callback[Msg]

	initialized bool
	cancel      *context.CancelFunc

	consumer *MarkableConsumer
	brokers  []string
	topic    string
	groupId  string
	config   *sarama.Config
}

func NewAckConsumeOperator[Msg any](serializer queue.MessageSerializer[*sarama.ConsumerMessage, Msg], ackCallback queue.AckCallback[Msg], callback queue.Callback[Msg], brokers []string, topic string, groupId string, config *sarama.Config) *AckConsumeOperator[Msg] {
	return &AckConsumeOperator[Msg]{
		serializer:  serializer,
		ackCallback: ackCallback,
		callback:    callback,
		brokers:     brokers,
		topic:       topic,
		groupId:     groupId,
		config:      config,
	}
}

func (k *AckConsumeOperator[Msg]) QueueName() string {
	return k.topic
}

func (k *AckConsumeOperator[Msg]) Serializer() queue.MessageSerializer[*sarama.ConsumerMessage, Msg] {
	return k.serializer
}

func (k *AckConsumeOperator[Msg]) AckCallback() queue.AckCallback[Msg] {
	return k.ackCallback
}

func (k *AckConsumeOperator[Msg]) Callback() queue.Callback[Msg] {
	return func(msg Msg) {
		k.callback(msg)
	}
}

// BeforeConsume 은 Serializer 를 호출한 후 AckCallback 함수를 실행하는 함수입니다.
// 실제 컨슘할 때에는 AckCallback 결과에 따라 Callback 함수를 실행할지 결정합니다.
func (k *AckConsumeOperator[Msg]) BeforeConsume(msg *sarama.ConsumerMessage, ack func()) (sMsg Msg, ok bool) {
	var err error
	sMsg, err = k.serializer.Serialize(msg)
	if err != nil {
		log.Fatal("Serializing error: ", err)
		return sMsg, false
	}

	if !k.ackCallback(sMsg) {
		return sMsg, false
	}

	ack()
	return sMsg, true
}

// Consume 은 Serializer 를 호출한 후 Callback 함수를 실행하는 함수입니다.
// 주의: 실제 컨슘할 때의 로직과는 다릅니다. 실제로는 BeforeConsume 함수를 통해 AckCallback 함수를 실행한 뒤 Ack 결과에 따라 Callback 을 실행합니다.
// ConsumeOperator 를 구현함과 동시에 테스트 가능성을 높이기 위해 이 함수를 따로 분리했습니다.
func (k *AckConsumeOperator[Msg]) Consume(msg *sarama.ConsumerMessage) {
	sMsg, err := k.serializer.Serialize(msg)
	if err != nil {
		return
	}

	k.callback(sMsg)
}

func (k *AckConsumeOperator[Msg]) Init() {
	consumer := NewMarkableConsumer(func(msg *sarama.ConsumerMessage, ack func()) {
		sMsg, ok := k.BeforeConsume(msg, ack)
		if !ok {
			return
		}

		// goroutine 으로 실행! BeforeConsume 이 완료되면 Consume된 상태이므로 goroutine으로 실행합니다..
		go k.callback(sMsg)
	})

	k.consumer = consumer
	k.initialized = true
}

func (k *AckConsumeOperator[Msg]) StartConsume() error {
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

func (k *AckConsumeOperator[Msg]) StopConsume() error {
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

	_, _, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: k.topic,
		Value: sarama.ByteEncoder(message),
	})

	if err != nil {
		return err
	}

	return nil
}
