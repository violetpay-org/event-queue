package queue

import "errors"

var (
	ErrBrokerNotFound = errors.New("broker not found")
	ErrTopicNotFound  = errors.New("topic not found")
	ErrStartConsume   = errors.New("something went wrong when starting consumption")
	ErrConsume        = errors.New("something went wrong when consuming message")
)
