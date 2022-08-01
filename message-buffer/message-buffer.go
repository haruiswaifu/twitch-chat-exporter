package messagebuf

import (
	"bytes"
	twitchIrc "github.com/gempir/go-twitch-irc/v2"
	"github.com/robfig/cron"
	log "github.com/sirupsen/logrus"
	awsClient "jinnytty-log-exporter/aws-client"
	"jinnytty-log-exporter/serializer"
	"sync"
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
	for c := range mbbcw.messageBuffersByChannel {
		mbbcw.messageBuffersByChannel[c] = []string{}
	}
}

func (mbbcw *messageBuffersByChannelWrapper) RoutinelyPushToS3() {
	c := cron.New()
	err := c.AddFunc("*/15 * * * *", func() {
		wg := sync.WaitGroup{}
		for c := range mbbcw.messageBuffersByChannel {
			wg.Add(1)
			go func(channel string) {
				defer wg.Done()
				fileContent := mbbcw.serialize(channel)
				err := mbbcw.awsClient.Put(channel, fileContent)
				if err != nil {
					log.Errorf("failed to write content to S3 for channel %s: %s", channel, err.Error())
					return
				}
			}(c)
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

func (mbbcw *messageBuffersByChannelWrapper) serialize(channel string) []byte {
	var fileContent bytes.Buffer
	for _, m := range mbbcw.messageBuffersByChannel[channel] {
		fileContent.WriteString(m)
		fileContent.WriteString("\n")
	}
	return fileContent.Bytes()
}
