package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/streadway/amqp"
)

type Connection struct {
	conn *amqp.Connection
	uri  string
}

func tryGetConnection(uri string) (*Connection, error) {
	for i := 0; i < 3; i++ {
		log.Printf("Essai N° %d\n", i+1)
		if c, err := amqp.Dial(uri); err == nil {
			conn := Connection{c, uri}
			return &conn, nil
		} else {
			log.Println(err.Error())
		}
		time.Sleep(3 * time.Second)
	}
	return nil, fmt.Errorf("No connection")
}

func tryPublish(c *Connection, msg *amqp.Publishing) error {
	ch, err := c.conn.Channel()
	if err != nil {
		return err
	}
	// Temporisation pour arrêter le serveur entretemps.
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Key ? ")
	_, err = reader.ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}

	err = ch.Publish("finance", "check", false, false, *msg)
	if err != nil {
		c, err = tryGetConnection(c.uri)
		if err != nil {
			return err
		}
		tryPublish(c, msg)
	}
	return err
}

func main() {

	// Si je gère les connexions  en dehors dans tryGetConnection ...
	c, err := tryGetConnection("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Connexion obtenue")
	defer c.conn.Close()

	// ... comment je gère les erreurs ici ?
	err = tryPublish(c, &amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte("RI5TO9O"),
	})
	if err != nil {
		log.Fatal(err)
	}
}
