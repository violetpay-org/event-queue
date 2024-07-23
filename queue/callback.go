package queue

type Callback[M any] func(req M) error
