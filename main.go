package main

import (
	"encoding/json"
	twitchIrc "github.com/gempir/go-twitch-irc/v2"
	"github.com/robfig/cron"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	awsClient "jinnytty-log-exporter/aws-client"
	messagebuf "jinnytty-log-exporter/message-buffer"
	"jinnytty-log-exporter/serializer"
	"time"
)

type secrets struct {
	Username string `json:"username"`
	OauthKey string `json:"oauth-key"`
}

var channels = []string{"jinnytty"}

func main() {
	secretsBytes, err := ioutil.ReadFile("./secrets.json")
	if err != nil {
		log.Fatalln("failed to read secrets")
	}

	s := &secrets{}
	err = json.Unmarshal(secretsBytes, s)
	if err != nil {
		log.Fatalln("failed to unmarshal secrets")
	}

	awsConfigBytes, err := ioutil.ReadFile("./aws-config.json")
	if err != nil {
		log.Fatalln("failed to read aws config")
	}

	awsConf := &awsClient.AwsConfig{}
	err = json.Unmarshal(awsConfigBytes, awsConf)
	if err != nil {
		log.Fatalln("failed to unmarshal aws config")
	}

	client := twitchIrc.NewClient(s.Username, s.OauthKey)
	client.Join(channels...)

	client.OnConnect(func() {
		log.Println("connected")
	})

	awsClnt := awsClient.NewAWSClient(*awsConf)
	messageBuffer := messagebuf.NewMessageBuffer(channels[0], awsClnt)
	client.OnPrivateMessage(func(m twitchIrc.PrivateMessage) {
		message, err := serializer.ToLine(m)
		if err != nil {
			log.Errorf("failed to serialize message %s", m.Message)
		}
		messageBuffer.Add(message)
	})

	routinelyPostTopChatters(client, awsClnt)

	err = client.Connect()
	if err != nil {
		log.Fatalf("failed to connect: %s", err)
	}
}

func routinelyPostTopChatters(twitchClient *twitchIrc.Client, awsClient *awsClient.AWSClient) {
	c := cron.New()
	err := c.AddFunc("@daily", func() {
		chatters, err := awsClient.GetTopChatters(time.Now().Add(-time.Hour))
		if err != nil {
			log.Errorf("failed to get top chatters: %s", err.Error())
		}
		twitchClient.Say(channels[0], chatters)
	})
	if err != nil {
		log.Errorf("failed to add cron function: %s", err)
	}
	c.Start()
}
