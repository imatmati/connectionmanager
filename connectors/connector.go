package connectors

import (
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/streadway/amqp"
)

var Synchro sync.WaitGroup

type Connector struct {
	url      string
	pubConn  *amqp.Connection
	consConn *amqp.Connection
}

func setConnection(conn **amqp.Connection, url, name string) {

	log.Printf("Trying to connect '%s' to URL %s\n", name, url)
	log.Printf("Current connexion pointer %p\n", *conn)
	var connected bool
	for i := 0; i < 3; i++ {
		fmt.Println("Hit a key to connect to RabbitMQ")
		fmt.Scanln()
		if c, err := amqp.Dial(url); err == nil {
			log.Println("Connection acquired")
			*conn = c
			log.Printf("New connection pointer %p\n", *conn)
			connected = true
			receiver := make(chan *amqp.Error)
			c.NotifyClose(receiver)

			go func() {
				e := <-receiver
				log.Printf("Lost connection for %s : reconnecting : reason %s\n", name, e.Error())
				setConnection(conn, url, name)
				Synchro.Done()
			}()
			break
		}
	}
	if !connected {
		panic(errors.New("Connection failed"))
	}
}

func (cm Connector) GetPublishConnection() **amqp.Connection {
	Synchro.Add(2)
	// Mutex ?
	if cm.pubConn == nil {
		setConnection(&cm.pubConn, cm.url, "publish")
	}
	fmt.Printf("pub conn %p\n", cm.pubConn)
	return &cm.pubConn
}

func (cm Connector) GetConsumeConnection() **amqp.Connection {
	// Mutex ?
	if cm.consConn == nil {
		setConnection(&cm.consConn, cm.url, "consume")
	}
	return &cm.consConn
}

func New(url string) Connector {
	return Connector{url, nil, nil}
}
