package queue

type Consumer[InMsg any, Msg any] interface {
	QueueName() string
	Serializer() MessageSerializer[InMsg, Msg]
	Callback() Callback[Msg]
	StartConsume() error
	StopConsume() error
}

type ProduceOperator[Msg any] interface {
	QueueName() string
	Produce(message Msg) error
}
