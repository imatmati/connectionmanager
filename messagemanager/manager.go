package messagemanager

import (
	"bufio"
	"context"
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
	lastErr  error
	ctx      context.Context
	executed chan interface{}
}

var PubConn *amqp.Connection

//var ConsConn *amqp.Connection
var wg sync.WaitGroup

func init() {
	wg.Add(1)
	setConnection(&PubConn, "'publish connection'")
	//setConnection(&ConsConn, "'consume connection'")
}

func setConnection(conn **amqp.Connection, name string) {
	fmt.Printf("setting connection for %s\n", name)
	fmt.Printf("current connexion pointer %p\n", *conn)
	var connected bool
	for i := 0; i < 3; i++ {
		reader := bufio.NewReader(os.Stdin)
		fmt.Printf("Strike a key to proceed connection for %s:", name)
		reader.ReadString('\n')
		if c, err := amqp.Dial("amqp://guest:guest@localhost:5672/"); err == nil {
			fmt.Println("connection acquired")
			*conn = c
			fmt.Printf("new connection pointer %p\n", *conn)
			connected = true
			receiver := make(chan *amqp.Error)
			c.NotifyClose(receiver)

			go func() {
				e := <-receiver
				fmt.Printf("Lost connection for %s : reconnecting : reason %s\n", name, e.Error())
				setConnection(conn, name)
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
	fmt.Println("==== Publish ===")
	call.executed = make(chan interface{})
	var cancelFunc context.CancelFunc
	call.ctx, cancelFunc = context.WithTimeout(context.Background(), call.Timeout)
	go acquireConnectionAndProceed(call)
	go func() {
		select {
		case <-call.ctx.Done():
			fmt.Println("Timeout happened")
			call.Err <- errors.New("Timeout happened")
			cancelFunc()
			fmt.Println("Timeout sent")
		case <-call.executed:
			fmt.Println("Execution done")
			cancelFunc()
		}
	}()

}

// Acquérir les connexions depuis un pool pour producteur/consommateur
func acquireConnectionAndProceed(call Call) {
	fmt.Println("==== acquireConnectionAndProceed ===")
	call.Conn = &PubConn
	acquireChannelAndProceed(call)
}

func acquireChannelAndProceed(call Call) {
	fmt.Println("==== acquireChannelAndProceed ===")
	if call.Retry <= 0 {
		fmt.Println("==== Error Max Retries ===")
		call.Err <- fmt.Errorf("max retries exceeded after : %s", call.lastErr.Error())
		return
	}
	if ch, err := (*call.Conn).Channel(); err == nil {
		call.Channel = ch
		fmt.Println("Please stop the rabbitmq server and then restart it after reconnection process has begun.")
		reader := bufio.NewReader(os.Stdin)
		fmt.Println("Strike a key")
		reader.ReadString('\n')
		wg.Wait()
		publishAndProceed(call)
	} else {
		call.Retry--
		call.lastErr = err
		acquireChannelAndProceed(call)
	}

}

func publishAndProceed(call Call) {
	fmt.Println("==== publishAndProceed ===")
	if call.Retry <= 0 {
		fmt.Println("==== Error Max Retries ===")
		call.Err <- fmt.Errorf("max retries exceeded after : %s", call.lastErr.Error())
		return
	}

	if err := call.Channel.Publish(call.Exchange, call.Key, false, false, *call.Msg); err == nil {
		consumeResponse(call)
	} else {
		call.Retry--
		acquireChannelAndProceed(call)
	}

}

func consumeResponse(call Call) {
	fmt.Println("==== consumeResponse ===")
	close(call.Done)
	fmt.Println("closed call.Done")
	close(call.executed)
	fmt.Println("closed call.executed")
}
