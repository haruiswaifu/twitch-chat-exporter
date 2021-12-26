package s3_client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	twitchIrc "github.com/gempir/go-twitch-irc/v2"
	"time"
)

const bucketName = "twitch-channel-logs"

type S3ClientInterface interface {
	Put(message string, channel string, time time.Time) error
}

type S3Client struct {
	s3uploader *s3manager.Uploader
}

func NewS3Client() *S3Client {
	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials
	// and region from the shared configuration file ~/.aws/config.
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Profile:           "s3-user",
	}))
	uploader := s3manager.NewUploader(sess)
	return &S3Client{
		s3uploader: uploader,
	}
}

type messageToSave struct {
	Text      string `json:"message"`
	Timestamp int64  `json:"timestamp"`
	Username  string `json:"username"`
	Channel   string `json:"channel"`
}

func (s3 *S3Client) Put(message twitchIrc.PrivateMessage) error {
	t, channel, user := message.Time, message.Channel, message.User.Name

	y, month, d := t.Year(), t.Month(), t.Day()
	h, min, sec := t.Hour(), t.Minute(), t.Second()

	prefixKey := fmt.Sprintf("channel=%s/user=%s/year=%d/month=%s/day=%d/hour=%d/min=%d/sec=%d/message", channel, user, y, month, d, h, min, sec)

	m := messageToSave{
		Text:      message.Message,
		Timestamp: message.Time.Unix(),
		Username:  message.User.Name,
		Channel:   message.Channel,
	}
	marshalledMessage, err := json.Marshal(m)
	if err != nil {
		errMessage := fmt.Sprintf("failed to marshal message: %s", err)
		return errors.New(errMessage)
	}

	_, err = s3.s3uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(prefixKey),
		Body:   bytes.NewReader(marshalledMessage),
	})

	if err != nil {
		errMessage := fmt.Sprintf("failed to put chat message in s3 bucket: %s", err)
		return errors.New(errMessage)
	}

	return nil

}
