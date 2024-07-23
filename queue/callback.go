package queue

// Callback 는 메시지를 처리하는 콜백 함수입니다.
type Callback[M any] func(req M)

// AckCallback 는 Acknowledge 를 처리하는 콜백 함수 타입입니다. 실행되는 흐름은 아래와 같습니다.
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
type AckCallback[M any] func(req M) bool
