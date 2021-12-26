package s3_client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	twitchIrc "github.com/gempir/go-twitch-irc/v2"
	"time"
)

const bucketName = "twitch-channel-logs"
const athenaDbName = "twitch_chat_logs_test"
const athenaTableName = "logs"
const queryExecutionBucketName = "twitch-channel-logs-athena-results"

type S3ClientInterface interface {
	Put(message string, channel string, time time.Time) error
}

type S3Client struct {
	s3uploader   *s3manager.Uploader
	athenaClient *athena.Athena
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
	athenaClient := athena.New(sess)
	return &S3Client{
		s3uploader:   uploader,
		athenaClient: athenaClient,
	}
}

type messageToSave struct {
	Text     string `json:"message"`
	Time     string `json:"time"`
	Username string `json:"username"`
	Channel  string `json:"channel"`
}

func (s3 *S3Client) Put(message twitchIrc.PrivateMessage) error {
	t, channel := message.Time, message.Channel

	y, m, d := t.Year(), int(t.Month()), t.Day()
	dateString := fmt.Sprintf("%d-%d-%d", y, m, d)
	logIdentifier := fmt.Sprintf("%s.log", t.String())
	prefixKey := fmt.Sprintf("channel=%s/date_string=%s/%s", channel, dateString, logIdentifier)

	mess := messageToSave{
		Text:     message.Message,
		Time:     t.String(),
		Username: message.User.Name,
		Channel:  channel,
	}
	marshalledMessage, err := json.Marshal(mess)
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

	var s athena.StartQueryExecutionInput
	var queryString = fmt.Sprintf("ALTER TABLE %s.%s ADD IF NOT EXISTS PARTITION (channel = '%s', date_string = '%s');", athenaDbName, athenaTableName, channel, dateString)
	s.SetQueryString(queryString)

	var q athena.QueryExecutionContext
	q.SetDatabase(athenaDbName)

	s.SetQueryExecutionContext(&q)

	var r athena.ResultConfiguration
	queryExecutionBucketLocation := fmt.Sprintf("s3://%s", queryExecutionBucketName)
	r.SetOutputLocation(queryExecutionBucketLocation)

	s.SetResultConfiguration(&r)

	_, err = s3.athenaClient.StartQueryExecution(&s)
	if err != nil {
		errMessage := fmt.Sprintf("failed to create athena partition for chat message: %s", err)
		return errors.New(errMessage)
	}

	return nil

}
