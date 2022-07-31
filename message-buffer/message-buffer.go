package messagebuf

import (
	"bytes"
	log "github.com/sirupsen/logrus"
	awsClient "jinnytty-log-exporter/aws-client"
	"time"
)

type messageBuffer struct {
	messages  []string
	awsClient *awsClient.AWSClient
}

func NewMessageBuffer(channel string, awsClient *awsClient.AWSClient) *messageBuffer {
	mb := &messageBuffer{
		messages:  []string{},
		awsClient: awsClient,
	}
	go mb.RoutinelyPushToS3(channel, 10*time.Minute)
	return mb
}

func (mb *messageBuffer) Add(message string) {
	mb.messages = append(mb.messages, message)
}

func (mb *messageBuffer) Clear() {
	mb.messages = []string{}
}

func (mb *messageBuffer) RoutinelyPushToS3(channel string, interval time.Duration) {
	for {
		time.Sleep(interval)
		fileContent := mb.serialize()
		err := mb.awsClient.Put(channel, fileContent)
		if err != nil {
			log.Errorf("failed to write content to S3: %s", err.Error())
			continue
		}
		mb.Clear()
	}
}

func (mb *messageBuffer) serialize() []byte {
	var fileContent bytes.Buffer
	for _, m := range mb.messages {
		fileContent.WriteString(m)
		fileContent.WriteString("\n")
	}
	return fileContent.Bytes()
}
