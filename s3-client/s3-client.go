package s3_client

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"strconv"
	"time"
)

const bucketName = "twitch-channel-logs"

type S3ClientInterface interface {
	Put(message string, channel string, time time.Time) error
}

type S3Client struct{}

func (s3 *S3Client) Put(message string, user string, channel string, time time.Time) error {
	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials
	// and region from the shared configuration file ~/.aws/config.
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Profile:           "s3-user",
	}))

	uploader := s3manager.NewUploader(sess)

	y, month, d := time.Year(), time.Month(), time.Day()
	h, min, sec := time.Hour(), time.Minute(), time.Second()

	prefixKey := fmt.Sprintf("channel=%s/user=%s/year=%d/month=%s/day=%d/hour=%d/min=%d/sec=%d/message", channel, user, y, month, d, h, min, sec)

	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(prefixKey),
		Body:   bytes.NewReader([]byte(message)),
	})

	if err != nil {
		errMessage := fmt.Sprintf("failed to put chat message in s3 bucket: %s", err)
		return errors.New(errMessage)
	}

	return nil

}

func convertAllStringToInt(numbers []string) ([]int, error) {
	numbersAsInts := []int{}
	for _, number := range numbers {
		numberAsInt, err := strconv.Atoi(number)
		if err != nil {
			return []int{}, errors.New("failed to convert at least one number")
		}
		numbersAsInts = append(numbersAsInts, numberAsInt)
	}
	return numbersAsInts, nil
}
