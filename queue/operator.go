package queue

type ConsumeOperator[InMsg any, Msg any] interface {
	QueueName() string
	Serializer() MessageSerializer[InMsg, Msg]
	Callback() Callback[Msg]
	Consume(message InMsg)
	StartConsume() error
	StopConsume() error
}

type ProduceOperator[Msg any] interface {
	QueueName() string
	Produce(message Msg) error
}
