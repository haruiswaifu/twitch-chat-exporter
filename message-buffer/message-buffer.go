package messagebuf

import (
	"bytes"
	"fmt"
	twitchIrc "github.com/gempir/go-twitch-irc/v2"
	"github.com/robfig/cron/v3"
	log "github.com/sirupsen/logrus"
	awsClient "jinnytty-log-exporter/aws-client"
	"jinnytty-log-exporter/serializer"
	"sync"
)

type messageBuffersByChannelWrapper struct {
	messageBuffersByChannel map[string][]string
	awsClient               *awsClient.AWSClient
	lock                    sync.RWMutex
}

func (mbbcw *messageBuffersByChannelWrapper) Print() {
	mbbcw.lock.Lock()
	defer mbbcw.lock.Unlock()
	for k, v := range mbbcw.messageBuffersByChannel {
		fmt.Printf("%d messages for channel %s\n", len(v), k)
	}
}

func NewMessageBuffer(channels []string, awsClient *awsClient.AWSClient) *messageBuffersByChannelWrapper {
	mbbc := map[string][]string{}
	for _, c := range channels {
		mbbc[c] = []string{}
	}
	mbbcw := &messageBuffersByChannelWrapper{
		messageBuffersByChannel: mbbc,
		awsClient:               awsClient,
		lock:                    sync.RWMutex{},
	}
	mbbcw.RoutinelyPushToS3()
	return mbbcw
}

func (mbbcw *messageBuffersByChannelWrapper) Add(m twitchIrc.PrivateMessage) {
	message, err := serializer.ToLine(m)
	if err != nil {
		log.Errorf("failed to serialize message %s", m.Message)
	}
	mbbcw.messageBuffersByChannel[m.Channel] = append(mbbcw.messageBuffersByChannel[m.Channel], message)
}

func (mbbcw *messageBuffersByChannelWrapper) Clear() {
	mbbcw.lock.Lock()
	defer mbbcw.lock.Unlock()
	for c := range mbbcw.messageBuffersByChannel {
		mbbcw.messageBuffersByChannel[c] = []string{}
	}
}

func (mbbcw *messageBuffersByChannelWrapper) RoutinelyPushToS3() {
	c := cron.New()
	_, err := c.AddFunc("*/15 * * * *", func() {
		wg := sync.WaitGroup{}
		for iteratedChannel, messages := range mbbcw.messageBuffersByChannel {
			wg.Add(1)
			go func(channel string, messages []string) {
				defer wg.Done()
				fileContent := serialize(messages)
				err := mbbcw.awsClient.Put(channel, fileContent)
				if err != nil {
					log.Errorf("failed to write content to S3 for channel %s: %s", channel, err.Error())
					return
				}
				log.Infof("put %d bytes into channel %s", len(fileContent), channel)
			}(iteratedChannel, messages)
		}
		wg.Wait()
		mbbcw.Clear()
	})
	if err != nil {
		log.Errorf("failed to create cron job for pushing to S3: %s", err)
		return
	}
	c.Start()
}

func serialize(messages []string) []byte {
	var fileContent bytes.Buffer
	for _, m := range messages {
		fileContent.WriteString(m)
		fileContent.WriteString("\n")
	}
	return fileContent.Bytes()
}
