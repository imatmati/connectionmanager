package messagemanager

import (
	"errors"
	"time"

	"github.com/streadway/amqp"
)

//Call transports a call with its state through calling process
type Call struct {
	Conn     *amqp.Connection
	Channel  *amqp.Channel
	Exchange string
	Key      string
	Msg      *amqp.Publishing
	Resp     chan<- []byte
	Err      chan<- error
	Done     chan<- interface{}
	CorrID   string
	Retry    int
	Timeout  time.Duration
}

var PubConn *amqp.Connection
var ConsConn *amqp.Connection

func init() {
	getConnection(&PubConn)
	getConnection(&ConsConn)
}

func getConnection(conn **amqp.Connection) {
	var connected bool
	for i := 0; i < 3; i++ {
		if c, err := amqp.Dial("amqp://guest:guest@localhost:5672/"); err == nil {
			*conn = c
			connected = true
			receiver := make(chan *amqp.Error)
			c.NotifyClose(receiver)

			go func() {
				<-receiver
				getConnection(conn)
			}()
			break
		}
	}
	if !connected {
		panic(errors.New("Connection failed"))
	}
}

//Publish publishes message
func Publish(call Call) {
	go acquireConnectionAndProceed(call)
}

// Acquérir les connexions depuis un pool pour producteur/consommateur
func acquireConnectionAndProceed(call Call) {
	var (
		err  error
		conn *amqp.Connection
	)
	for i := 0; i < call.Retry; i++ {
		if conn, err = amqp.Dial("amqp://guest:guest@localhost:5672/"); err == nil {
			call.Conn = conn
			manageClosedConnection(call)
			acquireChannelAndProceed(call)
			return
		}
		// Gérer le timeout avec un time.After
		time.Sleep(call.Timeout / 3)
		call.Retry--
	}
	call.Err <- err
}

func manageClosedConnection(call Call) {
	receiver := make(chan *amqp.Error)
	call.Conn.NotifyClose(receiver)

	go func() {
		<-receiver
		acquireConnectionAndProceed(call)
	}()

}

func acquireChannelAndProceed(call Call) {

	if ch, err := call.Conn.Channel(); err == nil {
		call.Channel = ch
		publishAndProceed(call)
	} else {
		// Il faut interposer un channel pour ne laisser passer les erreurs que si on ne se reconnecte pas.
		call.Err <- err
	}
}

func publishAndProceed(call Call) {

	if err := call.Channel.Publish(call.Exchange, call.Key, false, false, *call.Msg); err == nil {
		consumeResponse(call)
	} else {
		call.Err <- err
	}

}

func consumeResponse(call Call) {
	close(call.Done)
}
