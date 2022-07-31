package aws_client

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"math"
	"strings"
	"time"
)

type AwsConfig struct {
	BucketName               string `json:"bucket-name"`
	AthenaDbName             string `json:"athena-db-name"`
	AthenaTableName          string `json:"athena-table-name"`
	QueryExecutionBucketName string `json:"query-execution-bucket-name"`
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
		Profile:           "default",
	}))
	uploader := s3manager.NewUploader(sess)
	athenaClient := athena.New(sess)
	return &AWSClient{
		s3uploader:   uploader,
		athenaClient: athenaClient,
		awsConfig:    awsConfig,
	}
}

func (awsClient *AWSClient) Put(channel string, fileContent []byte) error {
	currentTime := time.Now()
	dateString := currentTime.Format("2006-01-02")
	logIdentifier := fmt.Sprintf("%s.log", currentTime.String())
	prefixKey := fmt.Sprintf("channel=%s/date_string=%s/%s", channel, dateString, logIdentifier)

	_, err := awsClient.s3uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(awsClient.awsConfig.BucketName),
		Key:    aws.String(prefixKey),
		Body:   bytes.NewReader(fileContent),
	})

	if err != nil {
		errMessage := fmt.Sprintf("failed to put chat message in s3 bucket: %s", err)
		return errors.New(errMessage)
	}

	queryTemplate := `ALTER TABLE %s.%s
ADD IF NOT EXISTS PARTITION (channel = '%s', date_string = '%s');`
	queryString := fmt.Sprintf(queryTemplate,
		awsClient.awsConfig.AthenaDbName,
		awsClient.awsConfig.AthenaTableName,
		channel,
		dateString)
	queryExecutionContext := &athena.QueryExecutionContext{
		Database: aws.String(awsClient.awsConfig.AthenaDbName),
	}
	queryExecutionBucketLocation := fmt.Sprintf("s3://%s", awsClient.awsConfig.QueryExecutionBucketName)
	queryInput := &athena.StartQueryExecutionInput{
		QueryString:           aws.String(queryString),
		QueryExecutionContext: queryExecutionContext,
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: aws.String(queryExecutionBucketLocation),
		},
	}

	_, err = awsClient.athenaClient.StartQueryExecution(queryInput)
	if err != nil {
		errMessage := fmt.Sprintf("failed to create athena partition for chat message: %s", err)
		return errors.New(errMessage)
	}
	return nil
}

func (awsClient *AWSClient) GetTopChatters(startTime time.Time, endTime time.Time) (string, error) {
	startDateString := startTime.Format("2006-01-02")
	endDateString := endTime.Format("2006-01-02")
	dayDiff := endTime.Sub(startTime).Hours() / 24

	queryTemplate := `SELECT COUNT(*) AS messages, username
FROM twitch_chat_logs_test.logs
WHERE date_string BETWEEN '%s' AND '%s'
GROUP BY username
ORDER BY messages DESC
LIMIT 10;`

	queryString := fmt.Sprintf(queryTemplate, startDateString, endDateString)
	queryExecutionContext := &athena.QueryExecutionContext{
		Database: aws.String(awsClient.awsConfig.AthenaDbName),
	}
	queryExecutionBucketLocation := fmt.Sprintf("s3://%s", awsClient.awsConfig.QueryExecutionBucketName)
	queryInput := &athena.StartQueryExecutionInput{
		QueryString:           aws.String(queryString),
		QueryExecutionContext: queryExecutionContext,
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: aws.String(queryExecutionBucketLocation),
		},
	}
	result, err := awsClient.athenaClient.StartQueryExecution(queryInput)
	if err != nil {
		return "", fmt.Errorf("failed to start query: %w", err)
	}

	time.Sleep(1 * time.Minute)

	input := &athena.GetQueryResultsInput{
		QueryExecutionId: result.QueryExecutionId,
	}
	results, err := awsClient.athenaClient.GetQueryResults(input)
	if err != nil {
		return "", fmt.Errorf("failed to get query results: %w", err)
	}

	resultStringBuilder := strings.Builder{}
	resultStringBuilder.WriteString(fmt.Sprintf("Top chatters of the last %d days: ", int(math.Trunc(dayDiff))))
	for i, row := range results.ResultSet.Rows {
		if i == 0 {
			continue
		}
		resultStringBuilder.WriteString(fmt.Sprintf("#%d: %s (%s)", i, *row.Data[1].VarCharValue, *row.Data[0].VarCharValue))
		if i != len(results.ResultSet.Rows)-1 {
			resultStringBuilder.WriteString(", ")
		}
	}
	return resultStringBuilder.String(), nil
}
