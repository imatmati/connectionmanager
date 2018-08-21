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

//Reply manages a reply from RabbitMQ (RPC)
type Reply struct {
	resp     chan<- []byte
	Queue    string
	Consumer string
	consconn **amqp.Connection
}

//Call transports a call with its state through calling process
type Call struct {
	CorrelationID string
	Exchange      string
	Key           string
	Msg           *amqp.Publishing
	Retry         int
	Timeout       time.Duration
	pubconn       **amqp.Connection
}

//CallReply is a generic structure for calls to RabbitMQ with optional reply
type CallReply struct {
	call    Call
	reply   Reply
	err     chan<- error
	done    chan<- interface{}
	ctx     context.Context
	channel *amqp.Channel
	lastErr error
}

type Client struct {
	connector connectors.Connector
}

func (c Client) Publish(call Call) (<-chan error, <-chan interface{}) {

	errChan, doneChan, _ := c.publishConsume(&CallReply{
		call: call, reply: Reply{},
	})
	return errChan, doneChan
}

//PublishConsume publishes a message and consumes a reply from RabbitMQ
func (c Client) PublishConsume(callReply CallReply) (<-chan error, <-chan interface{}, <-chan []byte) {
	return c.publishConsume(&callReply)
}

//PublishConsume publishes a message and consumes a reply from RabbitMQ
func (c Client) publishConsume(callReply *CallReply) (<-chan error, <-chan interface{}, <-chan []byte) {

	// Mutex ?
	callReply.call.pubconn = c.connector.GetPublishConnection()
	callReply.reply.consconn = c.connector.GetConsumeConnection()

	errChan := make(chan error)
	doneChan := make(chan interface{})
	replyChan := make(chan []byte)

	callReply.err = errChan
	callReply.done = doneChan
	callReply.reply.resp = replyChan

	go acquireChannelAndProceed(callReply)
	go func() {
		var cancelFunc context.CancelFunc
		callReply.ctx, cancelFunc = context.WithTimeout(context.Background(), callReply.call.Timeout)
		<-callReply.ctx.Done()
		cancelFunc()
		callReply.err <- errors.New("Timeout happened")

	}()
	return errChan, doneChan, replyChan
}

//Publish and then let the process proceeds.
func publishAndProceed(callReply *CallReply) {
	fmt.Println("==== publishAndProceed ===")
	if err := checkRetry(callReply); err != nil {
		return
	}
	fmt.Println("Restart the server then hit a key")
	fmt.Scanln()
	connectors.Synchro.Wait()
	if err := callReply.channel.Publish(callReply.call.Exchange, callReply.call.Key, false, false, *callReply.call.Msg); err != nil {
		traceError(callReply, err)
		acquireChannelAndProceed(callReply)
		return
	}
	close(callReply.done)
}

//Acquire channel and then let the process proceeds.
func acquireChannelAndProceed(callReply *CallReply) {
	fmt.Println("==== acquireChannelAndProceed ===")
	if err := checkRetry(callReply); err != nil {
		return
	}

	if ch, err := (*callReply.call.pubconn).Channel(); err == nil {
		callReply.channel = ch
		publishAndProceed(callReply)
	} else {
		traceError(callReply, err)
		acquireChannelAndProceed(callReply)
	}

}

func traceError(callReply *CallReply, err error) {
	callReply.lastErr = err
	callReply.call.Retry = callReply.call.Retry - 1
}

func checkRetry(callReply *CallReply) error {
	var err error
	if callReply.call.Retry <= 0 {
		fmt.Println("==== Error Max Retries ===")
		err = fmt.Errorf("max retries exceeded after : %s", callReply.lastErr.Error())
		callReply.err <- err

	}
	return err
}
