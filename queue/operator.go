package queue

type ConsumeOperator[InMsg any, Msg any] interface {
	QueueName() string
	Serializer() MessageSerializer[InMsg, Msg]
	Callback() Callback[Msg]
	StartConsume() error
	StopConsume() error
}

type ProduceOperatorCtor[Msg any] interface {
	Dest(string) ProduceOperator[Msg]
}

type ProduceOperator[Msg any] interface {
	Produce(message Msg) error
}
