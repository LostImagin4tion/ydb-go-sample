package topic

import (
	"context"
	"io"
	"log"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

type ReaderWorker struct {
	topicReader *topicreader.Reader
	quitChannel chan bool
}

func NewReaderWorker(topicClient topic.Client) (*ReaderWorker, error) {
	var reader, err = topicClient.StartReader(
		"email",
		topicoptions.ReadTopic("task_status"),
	)
	if err != nil {
		return nil, err
	}

	return &ReaderWorker{
		topicReader: reader,
		quitChannel: make(chan bool),
	}, nil
}

func (w *ReaderWorker) Run(ctx context.Context) {
	var goroutine = func() {
		for true {
			var message, err = w.topicReader.ReadMessage(ctx)
			if err != nil {
				log.Printf("Error happened: %v\n", err)
				continue
			}

			content, err := io.ReadAll(message)
			if err != nil {
				log.Printf("Error happened: %v\n", err)
				continue
			}

			log.Printf("Received message: %v\n", string(content))

			err = w.topicReader.Commit(message.Context(), message)
			if err != nil {
				log.Printf("Error happened: %v\n", err)
				continue
			}

			if message.SeqNo == 6 {
				w.quitChannel <- true
				return
			}
		}
	}
	go goroutine()
}

func (w *ReaderWorker) Shutdown(ctx context.Context) error {
	<-w.quitChannel
	return w.topicReader.Close(ctx)
}
