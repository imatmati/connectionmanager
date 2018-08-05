package messagemanager

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

//Call transports a call with its state through calling process
type Call struct {
	Conn     **amqp.Connection
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
var wg sync.WaitGroup

func init() {
	wg.Add(2)
	setConnection(&PubConn)
	setConnection(&ConsConn)
}

func setConnection(conn **amqp.Connection) {
	fmt.Println("setConnection")
	fmt.Printf("id conn %p\n", *conn)
	var connected bool
	for i := 0; i < 3; i++ {
		fmt.Println("loop")
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("setConnection Enter text: ")
		reader.ReadString('\n')
		if c, err := amqp.Dial("amqp://guest:guest@localhost:5672/"); err == nil {
			fmt.Println("connection acquired")
			*conn = c
			fmt.Printf("id conn %p\n", *conn)
			connected = true
			receiver := make(chan *amqp.Error)
			c.NotifyClose(receiver)

			go func() {
				<-receiver
				setConnection(conn)
				wg.Done()
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

var i bool

// Acquérir les connexions depuis un pool pour producteur/consommateur
func acquireConnectionAndProceed(call Call) {
	call.Conn = &PubConn
	wg.Wait()
	acquireChannelAndProceed(call)
}

// func manageClosedConnection(call Call) {
// 	receiver := make(chan *amqp.Error)
// 	call.Conn.NotifyClose(receiver)
//
// 	go func() {
// 		<-receiver
// 		init()
// 		acquireConnectionAndProceed(call)
// 	}()
//
// }

var j int

func acquireChannelAndProceed(call Call) {
	if j < 4 {
		fmt.Printf("acquireChannelAndProceed id conn %p,%p,%p\n", call.Conn, PubConn, ConsConn)
		j++
	}

	if ch, err := (*call.Conn).Channel(); err == nil {
		call.Channel = ch
		publishAndProceed(call)
	} else {
		//fmt.Println(err.Error())
		acquireChannelAndProceed(call)
		// Il faut interposer un channel pour ne laisser passer les erreurs que si on ne se reconnecte pas.
		//call.Err <- err
	}
}

func publishAndProceed(call Call) {
	fmt.Println("publishAndProceed")
	if err := call.Channel.Publish(call.Exchange, call.Key, false, false, *call.Msg); err == nil {
		consumeResponse(call)
	} else {
		call.Err <- err
	}

}

func consumeResponse(call Call) {
	fmt.Println("consumeResponse")
	close(call.Done)
}
