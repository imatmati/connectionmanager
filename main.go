package main

import (
	"connectionmanager/messagemanager"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

func main() {
	errChan := make(chan error)
	respChan := make(chan []byte)
	doneChan := make(chan interface{})

	call := messagemanager.Call{
		Exchange: "finance",
		Key:      "check",
		Msg:      &amqp.Publishing{Body: []byte("RT84309"), ContentType: "text/plain"},
		Err:      errChan,
		Resp:     respChan,
		Done:     doneChan,
		Retry:    3,
		Timeout:  5000 * time.Millisecond,
	}
	messagemanager.Publish(call)
	select {
	case <-doneChan:
		fmt.Println("message envoyé")
	case <-respChan:
		fmt.Println("réponse reçue")
	case err := <-errChan:
		log.Println(err.Error())
	}
	time.Sleep(5 * time.Second)
}
