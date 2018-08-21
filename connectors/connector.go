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

func (cm Connector) GetPublishConnection() **amqp.Connection {
	Synchro.Add(1)
	// Mutex ?
	if cm.pubConn == nil {
		cm.setConnection(&cm.pubConn, "publish")
	}
	fmt.Printf("pub conn %p\n", cm.pubConn)
	return &cm.pubConn
}

func (cm Connector) GetConsumeConnection() **amqp.Connection {
	// Mutex ?
	if cm.consConn == nil {
		cm.setConnection(&cm.consConn, "publish")
	}
	return &cm.consConn
}

func (cm Connector) setConnection(conn **amqp.Connection, name string) {

	log.Printf("Trying to connect '%s' to URL %s\n", name, cm.url)
	log.Printf("Current connexion pointer %p\n", *conn)
	var connected bool
	for i := 0; i < 3; i++ {
		fmt.Println("Hit a key to connect to RabbitMQ")
		fmt.Scanln()
		if c, err := amqp.Dial(cm.url); err == nil {
			log.Println("Connection acquired")
			*conn = c
			log.Printf("New connection pointer %p\n", *conn)
			connected = true
			receiver := make(chan *amqp.Error)
			c.NotifyClose(receiver)

			go func() {
				e := <-receiver
				log.Printf("Lost connection for %s : reconnecting : reason %s\n", name, e.Error())
				cm.setConnection(conn, name)
				Synchro.Done()
			}()
			break
		}
	}
	if !connected {
		panic(errors.New("Connection failed"))
	}
}

func New(url string) Connector {
	return Connector{url, nil, nil}
}
