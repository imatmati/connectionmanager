package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/streadway/amqp"
)

func tryGetConnection(uri string) (conn *amqp.Connection, err error) {
	for i := 0; i < 3; i++ {
		log.Printf("Essai N° %d\n", i+1)
		if conn, err = amqp.Dial("amqp://guest:guest@localhost:5672/"); err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	return
}
func main() {

	// Si je gère les connexions  en dehors dans tryGetConnection ...
	conn, err := tryGetConnection("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Connexion obtenue")
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Key ? ")
	_, err = reader.ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}

	// ... comment je gère les erreurs ici ?
	err = ch.Publish("finance", "check", false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte("RI5TO9O"),
	})
	if err != nil {
		log.Fatal(err)
	}
}
