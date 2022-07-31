package main

import (
	"encoding/json"
	"errors"
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

	postChatterArgs := map[int]string{
		1: "daily",
		7: "weekly",
	}
	for daysBack, frequencyString := range postChatterArgs {
		err = routinelyPostTopChatters(client, awsClnt, daysBack)
		if err != nil {
			log.Errorf("failed to post %s top chatters: %s", frequencyString, err.Error())
		}
	}

	err = client.Connect()
	if err != nil {
		log.Fatalf("failed to connect: %s", err)
	}
}

func routinelyPostTopChatters(twitchClient *twitchIrc.Client, awsClient *awsClient.AWSClient, daysBack int) error {
	c := cron.New()
	var cronExpr string
	switch daysBack {
	case 1:
		cronExpr = "@daily"
	case 7:
		cronExpr = "@weekly"
	default:
		return errors.New("unimplemented other periods")
	}
	err := c.AddFunc(cronExpr, func() {
		yesterday := time.Now().Add(-time.Hour)
		startDay := yesterday.Add(-time.Hour * 24 * time.Duration(daysBack))
		chatters, err := awsClient.GetTopChatters(startDay, yesterday)
		if err != nil {
			log.Errorf("failed to get top chatters: %s", err.Error())
		}
		twitchClient.Say(channels[0], chatters)
	})
	if err != nil {
		log.Errorf("failed to add cron function: %s", err)
	}
	c.Start()
	return nil
}
