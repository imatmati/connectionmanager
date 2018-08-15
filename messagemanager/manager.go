package messagemanager

import (
	"account/utils/random"
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/streadway/amqp"
)

//Receive specifies
type Receive struct {
	Resp          chan<- []byte
	Queue         string
	Consumer      string
	CorrelationId string
}

//Call transports a call with its state through calling process
type Call struct {
	conn     **amqp.Connection
	channel  *amqp.Channel
	Exchange string
	Key      string
	Msg      *amqp.Publishing
	Receiver Receive
	Err      chan<- error
	Done     chan<- interface{}
	Retry    int
	Timeout  time.Duration
	lastErr  error
	ctx      context.Context
	executed chan interface{}
}

var pubConn *amqp.Connection

//var wg sync.WaitGroup

func init() {
	//wg.Add(1)
	setConnection(&pubConn, "'publish connection'")
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
				//wg.Done()
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
	if call.Receiver.Resp != nil {
		call.Receiver.CorrelationId = random.RandomString(32)
		fmt.Println("Correlation id", call.Receiver.CorrelationId)
	}
	call.executed = make(chan interface{})
	go acquireConnectionAndProceed(call)

	go func() {
		var cancelFunc context.CancelFunc
		call.ctx, cancelFunc = context.WithTimeout(context.Background(), call.Timeout)
		select {
		case <-call.ctx.Done():
			fmt.Println("Timeout happened")
			call.Err <- errors.New("Timeout happened")
			fmt.Println("Timeout sent")
		case <-call.executed:
			fmt.Println("Execution done")
		}
		cancelFunc()
	}()

}

// Acquire connections and then let the process proceeds.
func acquireConnectionAndProceed(call Call) {
	fmt.Println("==== acquireConnectionAndProceed ===")
	call.conn = &pubConn
	acquireChannelAndProceed(call)
}

//Acquire channel and then let the process proceeds.
func acquireChannelAndProceed(call Call) {
	fmt.Println("==== acquireChannelAndProceed ===")
	if err := checkRetry(call); err != nil {
		return
	}

	if ch, err := (*call.conn).Channel(); err == nil {
		call.channel = ch
		fmt.Println("Please stop the rabbitmq server and then restart it after reconnection process has begun.")
		reader := bufio.NewReader(os.Stdin)
		fmt.Println("Strike a key")
		reader.ReadString('\n')
		//wg.Wait()
		publishAndProceed(call)
	} else {
		call.Retry--
		call.lastErr = err
		acquireChannelAndProceed(call)
	}

}

func checkRetry(call Call) error {
	var err error
	if call.Retry <= 0 {
		fmt.Println("==== Error Max Retries ===")
		err = fmt.Errorf("max retries exceeded after : %s", call.lastErr.Error())
		call.Err <- err

	}
	return err
}

//Publis and then let the process proceeds.
func publishAndProceed(call Call) {
	fmt.Println("==== publishAndProceed ===")
	if err := checkRetry(call); err != nil {
		return
	}

	if err := call.channel.Publish(call.Exchange, call.Key, false, false, *call.Msg); err == nil {
		consumeResponse(call)
	} else {
		call.lastErr = err
		call.Retry--
		acquireChannelAndProceed(call)
	}

}

func consumeResponse(call Call) {
	if err := checkRetry(call); err != nil {
		return
	}
	fmt.Println("==== consumeResponse ===")
	if delivery, err := call.channel.Consume(call.Receiver.Queue, call.Receiver.Consumer, false, false, false, false, nil); err == nil {
		//for {
		// Que se passe-t-il en cas de perte de connexion à ce stade ?
		msg := <-delivery
		fmt.Printf("msg %s : %v\n", call.Receiver.CorrelationId, msg.CorrelationId)
		if msg.CorrelationId == call.Receiver.CorrelationId {
			msg.Ack(false)
			call.Receiver.Resp <- msg.Body
			//break
		}
		msg.Nack(false, true)
		//}
		call.Done <- nil
		fmt.Println("closed call.Done")
		close(call.executed)
		fmt.Println("closed call.executed")
	} else {
		call.lastErr = err
		call.Retry--
		consumeResponse(call)
		return
	}

}
