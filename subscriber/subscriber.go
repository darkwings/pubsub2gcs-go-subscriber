package subscriber

import (
	"fmt"
	"sync/atomic"
	"time"

	"encoding/json"

	"context"
	"io"

	"cloud.google.com/go/pubsub"

	"frank.com/gcp/storage"
)

type PublishedMessage struct {
	Id       string `json:"id" binding:"required"`
	Key      string `json:"key" binding:"required"`
	Content  string `json:"content" binding:"required"`
	Filename string `json:"filename" binding:"required"`
}

/*
  Ricezione messaggi in concorrenza. Posso gestire il numero delle goroutine che verranno
  istanziate (i canali sono gestiti internamente, presumo) e il numero massimo di messaggi
  outstanding (Unacked messages...) che posso gestire in modo concorrente
*/
func PullMsgsConcurrencyControl(w io.Writer, projectID, subID string) error {
	// projectID := "my-project-id"
	// subID := "my-sub"
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("pubsub.NewClient: %v", err)
	}
	defer client.Close()

	sub := client.Subscription(subID)
	// Must set ReceiveSettings.Synchronous to false (or leave as default) to enable
	// concurrency settings. Otherwise, NumGoroutines will be set to 1.
	sub.ReceiveSettings.Synchronous = false
	// NumGoroutines determines the number of goroutines sub.Receive will spawn to pull
	// messages.
	sub.ReceiveSettings.NumGoroutines = 16
	// MaxOutstandingMessages limits the number of concurrent handlers of messages.
	// In this case, up to 8 unacked messages can be handled concurrently.
	// N.B. Se c'è necessità di limitare il numero di messaggi Outstanding, può essere indice
	// che i messaggi vengono prodotti a ritmo troppo elevato per i consumer esistenti.
	// Può essere utile aumentare il numero di consumer.
	sub.ReceiveSettings.MaxOutstandingMessages = 8

	// Receive messages for 300 seconds.
	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()

	var counter int32

	// Receive blocks until the context is cancelled or an error occurs.

	// ========================================================================
	// TODO capire come mantenere aperta la connessione sempre e gestire errore

	err = sub.Receive(ctx, func(_ context.Context, msg *pubsub.Message) {
		// The message handler passed to Receive may be called concurrently
		// so it's okay to process the messages concurrently but make sure
		// to synchronize access to shared memory.

		content := string(msg.Data)
		var message PublishedMessage
		if err := json.Unmarshal(msg.Data, &message); err != nil {
			fmt.Fprintf(w, "Unmarshal error: %q\n", err)
		} else {
			fmt.Fprintf(w, "Got message: %q\n", content)
			fmt.Fprintln(w, "Attributes:")
			for key, value := range msg.Attributes {
				fmt.Fprintf(w, "%s = %s", key, value)
			}
			storage.UploadContent(w, "bucket_frank", message.Filename, content)
			atomic.AddInt32(&counter, 1)
		}

		// Ack un messaggio alla volta, a seconda del caso d'uso potrebbe essere necessario anche raggruppare messaggi
		msg.Ack()
	})
	if err != nil {
		return fmt.Errorf("pubsub: Receive returned error: %v", err)
	}
	fmt.Fprintf(w, "Received %d messages\n", counter)

	return nil
}

/*
 Esempio di utilizzo diretto di una goroutine + channel
*/
func PullMsgsCustomAttributes(w io.Writer, projectID, subID string) error {
	// projectID := "my-project-id"
	// subID := "my-sub"
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("pubsub.NewClient: %v", err)
	}
	defer client.Close()

	sub := client.Subscription(subID)

	// Receive messages for 10 seconds.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Create a channel to handle messages to as they come in.
	cm := make(chan *pubsub.Message)
	defer close(cm)

	// Handle individual messages in a goroutine.
	go func() {
		for msg := range cm {
			fmt.Fprintf(w, "Got message :%q\n", string(msg.Data))
			fmt.Fprintln(w, "Attributes:")
			for key, value := range msg.Attributes {
				fmt.Fprintf(w, "%s = %s", key, value)
			}
			msg.Ack()
		}
	}()

	// Receive blocks until the context is cancelled or an error occurs.
	err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		cm <- msg
	})
	if err != nil {
		return fmt.Errorf("Receive: %v", err)
	}

	return nil
}
