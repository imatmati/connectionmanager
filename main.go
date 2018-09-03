package main

import (
	"connectionmanager/clients"
	"coupecircuit/utils"
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
	// Deux connexions sont implicitement créées
	client := clients.New(URL)
	err, done := client.Publish(clients.Call{
		Exchange: "finance",
		Key:      "check",
		Msg:      &amqp.Publishing{Body: []byte("RT84309"), ContentType: "text/plain"},
		Retry:    3,
		Timeout:  10 * time.Second,
	})

	select {
	case e := <-err:
		log.Fatal(e.Error())
	case <-done:
		log.Println("Message sent")
	}

	err, done, resp := client.PublishConsume(
		clients.CallReply{
			Call: clients.Call{
				Exchange: "finance",
				Key:      "check",
				Msg:      &amqp.Publishing{Body: []byte("RT84309"), ContentType: "text/plain", CorrelationId: utils.RandomString(32)},
				Retry:    3,
				Timeout:  100 * time.Second,
			},
			Reply: clients.Reply{
				Queue:    "resp_check",
				Consumer: "",
			},
		})

	select {
	case e := <-err:
		log.Fatal(e.Error())
	case <-done:
		log.Println("Message sent")
		response := <-resp
		log.Printf("Response is %s\n", string(response))
	}
}
