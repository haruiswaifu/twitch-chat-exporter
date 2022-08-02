package aws_client

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/robfig/cron/v3"
	log "github.com/sirupsen/logrus"
	"math"
	"strings"
	"time"
)

const (
	dateStringFormat = "2006-01-02"
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

	return nil
}

func (awsClient *AWSClient) GetTopChatters(startTime time.Time, endTime time.Time, channel string) (string, error) {
	startDateString := startTime.Format("2006-01-02")
	endDateString := endTime.Format("2006-01-02")
	dayDiff := endTime.Sub(startTime).Hours() / 24

	queryTemplate := `SELECT COUNT(*) AS messages, username
FROM %s.%s
WHERE date_string BETWEEN '%s' AND '%s'
AND channel = '%s'
GROUP BY username
ORDER BY messages DESC
LIMIT 10;`

	queryString := fmt.Sprintf(queryTemplate, awsClient.awsConfig.AthenaDbName,
		awsClient.awsConfig.AthenaTableName, startDateString, endDateString, channel)
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

func (awsClient *AWSClient) CreateDailyPartition(channels []string) error {
	c := cron.New()
	_, err := c.AddFunc("0 0 */1 * *", func() {
		awsClient.createPartition(channels, 5)
	})
	if err != nil {
		return fmt.Errorf("failed to create cron job for creating athena partitions: %w", err)
	}
	c.Start()
	return nil
}

func createPartitionQueryTemplate(channels []string) string {
	var queryTemplateBuilder strings.Builder
	queryTemplateBuilder.WriteString("ALTER TABLE %s.%s ADD IF NOT EXISTS\n")
	for i, c := range channels {
		queryTemplateBuilder.WriteString(fmt.Sprintf("PARTITION (channel = '%s', date_string = '%%s')", c))
		if i != len(channels)-1 {
			queryTemplateBuilder.WriteString("\n")
		} else {
			queryTemplateBuilder.WriteString(";")
		}
	}
	queryTemplate := queryTemplateBuilder.String()
	return queryTemplate

}

func (awsClient *AWSClient) createPartition(channels []string, retries int) {
	if retries <= 0 {
		log.Errorln("reached max amount of retries to create athena partition")
		return
	}

	queryTemplate := createPartitionQueryTemplate(channels)
	dateString := time.Now().Format(dateStringFormat)

	queryString := fmt.Sprintf(queryTemplate,
		awsClient.awsConfig.AthenaDbName,
		awsClient.awsConfig.AthenaTableName,
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

	startQueryExecutionResult, err := awsClient.athenaClient.StartQueryExecution(queryInput)
	if err != nil {
		err := fmt.Errorf("failed to create athena partitions for date %s: %w", dateString, err)
		log.Println(err)
	}

	timeout := 30 * time.Minute
	interval := 1 * time.Minute
outer:
	for startTime := time.Now().Add(time.Minute); time.Now().Sub(startTime) < timeout; time.Sleep(interval) {
		getQueryExecutionInput := &athena.GetQueryExecutionInput{
			QueryExecutionId: startQueryExecutionResult.QueryExecutionId,
		}
		getQueryExecutionOutput, err := awsClient.athenaClient.GetQueryExecution(getQueryExecutionInput)
		if err != nil {
			log.Errorf("failed to get status of athena partition creation query execution: %s", err)
			continue
		}

		switch *getQueryExecutionOutput.QueryExecution.Status.State {
		case athena.QueryExecutionStateSucceeded:
			log.Infoln("successfully created athena partitions")
			return
		case athena.QueryExecutionStateFailed, athena.QueryExecutionStateCancelled:
			awsClient.createPartition(channels, retries-1)
			break outer
		case athena.QueryExecutionStateQueued, athena.QueryExecutionStateRunning:
			// wait for new status update
		}
	}

}
