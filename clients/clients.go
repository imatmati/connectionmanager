package clients

import (
	"connectionmanager/connectors"
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

func New(url string) Client {
	return Client{connector: connectors.New(url)}
}

//Reply manages a reply from RabbitMQ (RPC)
type Reply struct {
	Queue    string
	Consumer string
	resp     chan<- []byte
	consconn **amqp.Connection
}

//Call transports a call with its state through calling process
type Call struct {
	Exchange string
	Key      string
	Msg      *amqp.Publishing
	Retry    int
	Timeout  time.Duration
	pubconn  **amqp.Connection
}

//CallReply is a generic structure for calls to RabbitMQ with optional reply
type CallReply struct {
	Call    Call
	Reply   Reply
	err     chan<- error
	done    chan<- interface{}
	ctx     context.Context
	channel *amqp.Channel
	lastErr error
}

type Client struct {
	connector connectors.Connector
}

func (c *Client) Publish(call Call) (<-chan error, <-chan interface{}) {

	errChan, doneChan, _ := c.PublishConsume(CallReply{
		Call: call, Reply: Reply{},
	})
	return errChan, doneChan
}

//PublishConsume publishes a message and consumes a reply from RabbitMQ
func (c *Client) PublishConsume(callReply CallReply) (<-chan error, <-chan interface{}, <-chan []byte) {

	errChan := make(chan error)
	doneChan := make(chan interface{})
	replyChan := make(chan []byte)

	callReply.err = errChan
	callReply.done = doneChan
	callReply.Reply.resp = replyChan

	go func() {
		<-time.After(callReply.Call.Timeout)
		callReply.err <- errors.New("Timeout happened")
	}()

	go func() {
		callReply.Call.pubconn = c.connector.GetPublishConnection()
		if callReply.Reply.Queue != "" {
			callReply.Reply.consconn = c.connector.GetConsumeConnection()
		}
		acquireChannelAndProceed(callReply)
	}()
	return errChan, doneChan, replyChan
}

//Publish and then let the process proceeds.
func publishAndProceed(callReply CallReply) {
	fmt.Println("==== publishAndProceed ===")
	if err := checkRetry(callReply); err != nil {
		return
	}
	fmt.Println("Restart the server then hit a key")
	fmt.Scanln()
	connectors.Synchro.Wait()
	if err := callReply.channel.Publish(callReply.Call.Exchange, callReply.Call.Key, false, false, *callReply.Call.Msg); err != nil {
		traceError(callReply, err)
		acquireChannelAndProceed(callReply)
		return
	}
	if callReply.Reply.Queue != "" {
		consume(callReply)
	}

	close(callReply.done)
}

func consume(callReply CallReply) {
	fmt.Println("==== consume ===")
	if callReply.Call.Msg.CorrelationId == "" {
		panic(errors.New("Correlation id missing for consuming response"))
	}
	if delivery, err := callReply.channel.Consume(callReply.Reply.Queue, callReply.Reply.Consumer, false, true, true, true, nil); err != nil {
		traceError(callReply, err)
		acquireChannelAndProceed(callReply, consume)
		return

	} else {
	MsgLoop:
		for {
			select {
			case msg := <-delivery:
				log.Printf("Message received %s : %s\n", msg.CorrelationId, callReply.Call.Msg.CorrelationId)
				if msg.CorrelationId == callReply.Call.Msg.CorrelationId {
					log.Println("Correlation id matching")
					msg.Ack(false)
					log.Println("berfore sending")
					callReply.Reply.resp <- msg.Body
					log.Println("after sending")
					break MsgLoop
				} else {
					log.Println("Correlation id mismatching")
					msg.Nack(false, true)
				}

			}
		}
		fmt.Println("sortie de for")
	}
}

//Acquire channel and then let the process proceeds.
func acquireChannelAndProceed(callReply CallReply, fun ...func(CallReply)) {
	if len(fun) > 1 {
		panic(errors.New("One function allowed in acquireChannelAndProceed"))
	}
	f := publishAndProceed
	if len(fun) == 1 {
		f = fun[0]
	}
	fmt.Println("==== acquireChannelAndProceed ===")
	if err := checkRetry(callReply); err != nil {
		return
	}

	if ch, err := (*callReply.Call.pubconn).Channel(); err == nil {
		callReply.channel = ch
		f(callReply)
	} else {
		traceError(callReply, err)
		acquireChannelAndProceed(callReply, f)
	}

}

func traceError(callReply CallReply, err error) {
	log.Println(callReply)
	log.Println(err.Error())
	callReply.lastErr = err
	callReply.Call.Retry = callReply.Call.Retry - 1
}

func checkRetry(callReply CallReply) error {
	var err error
	if callReply.Call.Retry <= 0 {
		fmt.Println("==== Error Max Retries ===")
		err = fmt.Errorf("max retries exceeded after : %s", callReply.lastErr.Error())
		callReply.err <- err

	}
	return err
}
