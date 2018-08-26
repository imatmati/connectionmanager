package connectors

import (
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/streadway/amqp"
)

var Synchro sync.WaitGroup
var conSynchro sync.RWMutex

type Connector struct {
	url             string
	pubConn         *amqp.Connection
	consConn        *amqp.Connection
	pubConnVersion  int
	consConnVersion int
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
				//Synchro.Done()
			}()
			break
		}
	}
	if !connected {
		panic(errors.New("Connection failed"))
	}
}

func (cm Connector) GetConsumeConnection() **amqp.Connection {
	return getConnection(cm.consConn, "consume", cm.url, &cm.consConnVersion)
}

func (cm Connector) GetPublishConnection() **amqp.Connection {
	return getConnection(cm.pubConn, "publish", cm.url, &cm.pubConnVersion)
}

func getConnection(con *amqp.Connection, conName, url string, currentVersion *int) **amqp.Connection {
	fmt.Printf("getConnection for %s\n", conName)
	conSynchro.RLock()
	fmt.Println("RLock acquired")
	version := *currentVersion
	if con == nil {
		conSynchro.RUnlock()
		conSynchro.Lock()
		if version == *currentVersion {
			setConnection(&con, url, conName)
			// If here connection has been established otherwise would have panicked.
			*currentVersion++

		}
		conSynchro.Unlock()
	}

	fmt.Printf("pub conn %p\n", con)
	return &con
}

func New(url string) Connector {
	return Connector{url, nil, nil, 0, 0}
}
