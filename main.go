package main

import (
	"encoding/json"
	twitchIrc "github.com/gempir/go-twitch-irc/v2"
	"io/ioutil"
	awsClient "jinnytty-log-exporter/aws-client"
	"log"
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

	s3 := awsClient.NewAWSClient(*awsConf)

	client := twitchIrc.NewClient(s.Username, s.OauthKey)
	client.Join(channels...)

	client.OnConnect(func() {
		log.Println("connected")
	})

	client.OnPrivateMessage(func(m twitchIrc.PrivateMessage) {
		err := s3.Put(m)
		if err != nil {
			log.Println(err)
		}
	})

	err = client.Connect()
	if err != nil {
		log.Fatalf("failed to connect: %s", err)
	}
}
