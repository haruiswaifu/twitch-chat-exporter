package main

import (
	"encoding/json"
	"errors"
	twitchIrc "github.com/gempir/go-twitch-irc/v2"
	"github.com/robfig/cron/v3"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	awsClient "jinnytty-log-exporter/aws-client"
	messagebuf "jinnytty-log-exporter/message-buffer"
	"time"
)

type secrets struct {
	Username string `json:"username"`
	OauthKey string `json:"oauth-key"`
}

type env struct {
	Channels []string `yaml:"channels"`
}

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

	channelsBytes, err := ioutil.ReadFile("./env.yaml")
	if err != nil {
		log.Fatalln("failed to read env.yaml")
	}
	e := &env{}
	err = yaml.Unmarshal(channelsBytes, e)
	if err != nil {
		log.Fatalln("failed to unmarshal env.yaml")
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
	go joinChannels(client, e.Channels)

	client.OnConnect(func() {
		log.Println("connected")
	})

	awsClnt := awsClient.NewAWSClient(*awsConf)
	messageBuffer := messagebuf.NewMessageBuffer(e.Channels, awsClnt)
	client.OnPrivateMessage(func(m twitchIrc.PrivateMessage) {
		messageBuffer.Add(m)
	})

	for _, c := range e.Channels {
		err := awsClnt.CreateDailyPartition(e.Channels)
		if err != nil {
			log.Errorf("failed to start creating daily partition for %s: %s", c, err)
		}
	}

	postChatterArgs := map[int]string{
		1: "daily",
		7: "weekly",
	}
	for days, frequencyString := range postChatterArgs {
		err = routinelyPostTopChatters(client, awsClnt, days-1, e.Channels)
		if err != nil {
			log.Errorf("failed to post %s top chatters: %s", frequencyString, err.Error())
		}
	}

	err = client.Connect()
	if err != nil {
		log.Fatalf("failed to connect: %s", err)
	}
}

func joinChannels(client *twitchIrc.Client, channels []string) {
	for _, c := range channels {
		client.Join(c)
		log.Printf("joined channel #%s", c)
		time.Sleep(2 * time.Second) // avoid rate limits
	}
}

func routinelyPostTopChatters(twitchClient *twitchIrc.Client, awsClient *awsClient.AWSClient, daysBack int, channels []string) error {
	c := cron.New()
	var cronExpr string
	switch daysBack {
	case 1:
		cronExpr = "1 0 * * *" // 00:01 on any day
	case 7:
		cronExpr = "5 0 * * 1" // 00:05 on Mondays
	default:
		return errors.New("unimplemented other periods")
	}

	_, err := c.AddFunc(cronExpr, func() {
		results := map[string]string{}
		for _, channel := range channels {
			yesterday := time.Now().Add(-time.Hour)
			startDay := yesterday.Add(-time.Hour * 24 * time.Duration(daysBack))
			chatters, err := awsClient.GetTopChatters(startDay, yesterday, channel)
			if err != nil {
				log.Errorf("failed to get top chatters for channel %s: %s", channel, err.Error())
			}
			results[channel] = chatters
		}
		for channel, chatters := range results {
			twitchClient.Say(channel, chatters)
			time.Sleep(time.Second) // avoid rate limits
		}
	})
	if err != nil {
		log.Errorf("failed to add cron function: %s", err)
	}
	c.Start()
	return nil
}
