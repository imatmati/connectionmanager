package main

import (
	"connectionmanager/messagemanager"
	"fmt"

	"github.com/streadway/amqp"
)

func main() {

	done, err := messagemanager.Publish("finance", "check", &amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte("RT84309"),
	})
	defer close(done)
	defer close(err)

	select {
	case m := <-done:
		fmt.Println("message reçu", m)
	case e := <-err:
		if e != nil {
			fmt.Printf("Error : %s\n", e.Error())
		}
	}

}
