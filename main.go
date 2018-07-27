package main

import (
	"log"
	"time"

	"github.com/streadway/amqp"
)

func tryGetConnection(uri string) (conn *amqp.Connection, err error) {
	for i := 0; i < 3; i++ {
		log.Printf("Essai N° %d\n", i)
		if conn, err = amqp.Dial("amqp://guest:guest@localhost:5672/"); err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	return
}
func main() {

	conn, err := tryGetConnection("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Connexion obtenue")
	defer conn.Close()
}
