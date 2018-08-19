package main

import (
	"connectionmanager/clients"
	"log"
	"time"

	"github.com/streadway/amqp"
)

func main() {
	const (
		PUBLISH = "publish"
		URL     = "amqp://guest:guest@localhost:5672"
	)

	// Le client est relié à une instance RabbitMQ.
	// Deux connections sont implicitement créées
	client := clients.New(URL)
	err, done := client.Publish(clients.Call{
		Exchange: "finance",
		Key:      "check",
		Msg:      &amqp.Publishing{Body: []byte("RT84309"), ContentType: "text/plain"},
		Retry:    3,
		Timeout:  50000 * time.Millisecond,
	})

	select {
	case e := <-err:
		log.Fatal(e.Error())
	case <-done:
		log.Println("Message sent")
	}
}
