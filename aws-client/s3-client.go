package aws_client

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

type AwsConfig struct {
	BucketName               string `json:"bucket-name"`
	AthenaDbName             string `json:"athena-db-name"`
	AthenaTableName          string `json:"athena-table-name"`
	QueryExecutionBucketName string `json:"query-execution-bucket-name"`
}

type ChatLogPutter interface {
	Put(message string, channel string, time time.Time) error
}

type AWSClient struct {
	s3uploader   *s3manager.Uploader
	athenaClient *athena.Athena
	awsConfig    AwsConfig
}

func NewAWSClient(awsConfig AwsConfig) *AWSClient {
	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials
	// and region from the shared configuration file ~/.aws/config.
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Profile:           "s3-user",
	}))
	uploader := s3manager.NewUploader(sess)
	athenaClient := athena.New(sess)
	return &AWSClient{
		s3uploader:   uploader,
		athenaClient: athenaClient,
		awsConfig:    awsConfig,
	}
}

type messageToSave struct {
	Text     string `json:"message"`
	Time     string `json:"time"`
	Username string `json:"username"`
	Channel  string `json:"channel"`
}

func (awsClient *AWSClient) Put(message twitchIrc.PrivateMessage) error {
	t, channel := message.Time, message.Channel

	y, m, d := t.Year(), int(t.Month()), t.Day()
	dateString := fmt.Sprintf("%d-%d-%d", y, m, d)
	logIdentifier := fmt.Sprintf("%s.log", t.String())
	prefixKey := fmt.Sprintf("channel=%s/date_string=%s/%s", channel, dateString, logIdentifier)

	mess := messageToSave{
		Text:     message.Message,
		Time:     t.Format("02/01/06 15:04:05 MST"),
		Username: message.User.Name,
		Channel:  channel,
	}
	marshalledMessage, err := json.Marshal(mess)
	if err != nil {
		errMessage := fmt.Sprintf("failed to marshal message: %s", err)
		return errors.New(errMessage)
	}

	_, err = awsClient.s3uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(awsClient.awsConfig.BucketName),
		Key:    aws.String(prefixKey),
		Body:   bytes.NewReader(marshalledMessage),
	})

	if err != nil {
		errMessage := fmt.Sprintf("failed to put chat message in s3 bucket: %s", err)
		return errors.New(errMessage)
	}

	var s athena.StartQueryExecutionInput
	var queryString = fmt.Sprintf(
		"ALTER TABLE %s.%s ADD IF NOT EXISTS PARTITION (channel = '%s', date_string = '%s');",
		awsClient.awsConfig.AthenaDbName,
		awsClient.awsConfig.AthenaTableName,
		channel,
		dateString)
	s.SetQueryString(queryString)

	var q athena.QueryExecutionContext
	q.SetDatabase(awsClient.awsConfig.AthenaDbName)

	s.SetQueryExecutionContext(&q)

	var r athena.ResultConfiguration
	queryExecutionBucketLocation := fmt.Sprintf("s3://%s", awsClient.awsConfig.QueryExecutionBucketName)
	r.SetOutputLocation(queryExecutionBucketLocation)

	s.SetResultConfiguration(&r)

	_, err = awsClient.athenaClient.StartQueryExecution(&s)
	if err != nil {
		errMessage := fmt.Sprintf("failed to create athena partition for chat message: %s", err)
		return errors.New(errMessage)
	}

	return nil

}
