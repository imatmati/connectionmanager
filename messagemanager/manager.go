package messagemanager

import (
	"errors"

	"github.com/streadway/amqp"
)

//Publish publishes message
func Publish(exchange, key string, msg *amqp.Publishing) (chan bool, chan error) {
	response, err := make(chan bool), make(chan error)
	go func() { err <- errors.New("error") }()
	return response, err
}
