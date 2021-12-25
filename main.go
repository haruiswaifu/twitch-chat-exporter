package main

import (
	"encoding/json"
	twitchIrc "github.com/gempir/go-twitch-irc/v2"
	"io/ioutil"
	s3Client "jinnytty-log-exporter/s3-client"
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

	s3 := s3Client.S3Client{}

	client := twitchIrc.NewClient(s.Username, s.OauthKey)
	client.Join(channels...)

	client.OnConnect(func() {
		log.Println("connected")
	})

	client.OnPrivateMessage(func(m twitchIrc.PrivateMessage) {
		err := s3.Put(m.Message, m.User.Name, m.Channel, m.Time)
		if err != nil {
			log.Println(err)
		}
	})

	err = client.Connect()
	if err != nil {
		log.Fatalf("failed to connect: %s", err)
	}
}
