package messagebuf

import (
	"bytes"
	log "github.com/sirupsen/logrus"
	awsClient "jinnytty-log-exporter/aws-client"
	"sync"
	"time"
)

type messageBuffersByChannelWrapper struct {
	messageBuffersByChannel map[string][]string
	awsClient               *awsClient.AWSClient
}

func NewMessageBuffer(channels []string, awsClient *awsClient.AWSClient) *messageBuffersByChannelWrapper {
	mbbc := map[string][]string{}
	for _, c := range channels {
		mbbc[c] = []string{}
	}
	mbbcw := &messageBuffersByChannelWrapper{
		messageBuffersByChannel: mbbc,
		awsClient:               awsClient,
	}
	go mbbcw.RoutinelyPushToS3(10 * time.Minute)
	return mbbcw
}

func (mbbcw *messageBuffersByChannelWrapper) Add(message string, channel string) {
	mbbcw.messageBuffersByChannel[channel] = append(mbbcw.messageBuffersByChannel[channel], message)
}

func (mbbcw *messageBuffersByChannelWrapper) Clear() {
	for c := range mbbcw.messageBuffersByChannel {
		mbbcw.messageBuffersByChannel[c] = []string{}
	}
}

func (mbbcw *messageBuffersByChannelWrapper) RoutinelyPushToS3(interval time.Duration) {
	for {
		time.Sleep(interval)
		wg := sync.WaitGroup{}
		for c := range mbbcw.messageBuffersByChannel {
			wg.Add(1)
			go func() {
				defer wg.Done()
				fileContent := mbbcw.serialize(c)
				err := mbbcw.awsClient.Put(c, fileContent)
				if err != nil {
					log.Errorf("failed to write content to S3 for channel %s: %s", c, err.Error())
					return
				}
			}()
		}
		wg.Wait()
		mbbcw.Clear()
	}
}

func (mbbcw *messageBuffersByChannelWrapper) serialize(channel string) []byte {
	var fileContent bytes.Buffer
	for _, m := range mbbcw.messageBuffersByChannel[channel] {
		fileContent.WriteString(m)
		fileContent.WriteString("\n")
	}
	return fileContent.Bytes()
}
