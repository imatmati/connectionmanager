package main

import (
	m "connectionmanager/messagemanager"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

func main() {
	errChan := make(chan error)
	respChan := make(chan []byte)
	doneChan := make(chan interface{})

	call := m.Call{
		Exchange: "finance",
		Key:      "check",
		Msg:      &amqp.Publishing{Body: []byte("RT84309"), ContentType: "text/plain"},
		Err:      errChan,
		Receiver: m.Receive{
			Queue: "resp_check",
			Resp:  respChan,
		},
		Done:    doneChan,
		Retry:   3,
		Timeout: 50000 * time.Millisecond,
	}
	m.Publish(call)
	select {
	case <-doneChan:
		fmt.Println("message envoyé")
		resp := <-respChan
		fmt.Printf("réponse reçue : %v\n", resp)
	case err := <-errChan:
		log.Println(err.Error())
	}

}
