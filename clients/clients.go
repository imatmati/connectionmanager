package clients

import (
	"connectionmanager/connectors"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

func New(url string) Client {
	return Client{connector: connectors.New(url)}
}

//Receive specifies
type Receive struct {
	resp          chan<- []byte
	Queue         string
	Consumer      string
	CorrelationID string
}

//Call transports a call with its state through calling process
type Call struct {
	conn     **amqp.Connection
	channel  *amqp.Channel
	Exchange string
	Key      string
	Msg      *amqp.Publishing
	err      chan<- error
	receiver Receive
	done     chan<- interface{}
	Retry    int
	Timeout  time.Duration
	lastErr  error
	ctx      context.Context
	executed chan interface{}
}

type Client struct {
	connector connectors.Connector
}

func (c Client) Publish(call Call) (<-chan error, <-chan interface{}) {

	// Mutex ?
	call.conn = c.connector.GetPublishConnection()

	errChan := make(chan error)
	doneChan := make(chan interface{})

	call.err = errChan
	call.done = doneChan
	call.executed = make(chan interface{})
	go acquireChannelAndProceed(call)
	go func() {
		var cancelFunc context.CancelFunc
		call.ctx, cancelFunc = context.WithTimeout(context.Background(), call.Timeout)
		select {
		case <-call.ctx.Done():
			fmt.Println("Timeout happened")
			call.err <- errors.New("Timeout happened")
			fmt.Println("Timeout sent")
		case <-call.executed:
			fmt.Println("Execution done")
		}
		cancelFunc()
	}()
	return errChan, doneChan
}

//Publis and then let the process proceeds.
func publishAndProceed(call Call) {
	fmt.Println("==== publishAndProceed ===")
	if err := checkRetry(call); err != nil {
		return
	}
	fmt.Println("Restart the server then hit a key")
	fmt.Scanln()
	connectors.Synchro.Wait()
	if err := call.channel.Publish(call.Exchange, call.Key, false, false, *call.Msg); err != nil {
		traceError(call, err)
		acquireChannelAndProceed(call)
		return
	}
	close(call.done)
}

//Acquire channel and then let the process proceeds.
func acquireChannelAndProceed(call Call) {
	fmt.Println("==== acquireChannelAndProceed ===")
	if err := checkRetry(call); err != nil {
		return
	}

	if ch, err := (*call.conn).Channel(); err == nil {
		call.channel = ch
		publishAndProceed(call)
	} else {
		traceError(call, err)
		acquireChannelAndProceed(call)
	}

}

func traceError(call Call, err error) {
	call.lastErr = err
	call.Retry--
}

func checkRetry(call Call) error {
	var err error
	if call.Retry <= 0 {
		fmt.Println("==== Error Max Retries ===")
		err = fmt.Errorf("max retries exceeded after : %s", call.lastErr.Error())
		call.err <- err

	}
	return err
}
