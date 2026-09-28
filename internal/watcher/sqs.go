package watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/rs/zerolog/log"
)

// SQSConfig holds connection details for an SQS queue that receives S3 event notifications.
// The user is responsible for configuring S3 → SQS notification delivery in their AWS account.
type SQSConfig struct {
	QueueURL  string
	Region    string
	AccessKey string
	SecretKey string
}

// sqsClient is the subset of the SQS API used by SQSWatcher, allowing injection of fakes in tests.
type sqsClient interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

// SQSWatcher long-polls an SQS queue for S3 event notifications.
type SQSWatcher struct {
	client   sqsClient
	queueURL string
}

// NewSQS creates an SQSWatcher that polls the given queue for S3 object events.
func NewSQS(cfg SQSConfig) (*SQSWatcher, error) {
	opts := []func(*awscfg.LoadOptions) error{
		awscfg.WithRegion(cfg.Region),
	}
	if cfg.AccessKey != "" {
		opts = append(opts, awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		))
	}
	awsCfg, err := awscfg.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	return &SQSWatcher{
		client:   sqs.NewFromConfig(awsCfg),
		queueURL: cfg.QueueURL,
	}, nil
}

// Watch polls the SQS queue and emits an ObjectEvent for each S3 record received.
// Messages are deleted from the queue after parsing. The returned channel is closed
// when ctx is cancelled.
func (w *SQSWatcher) Watch(ctx context.Context, _ []string) (<-chan ObjectEvent, error) {
	out := make(chan ObjectEvent, 100)
	go func() {
		defer close(out)
		w.poll(ctx, out)
	}()
	return out, nil
}

func (w *SQSWatcher) poll(ctx context.Context, out chan<- ObjectEvent) {
	for {
		if ctx.Err() != nil {
			return
		}
		resp, err := w.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(w.queueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     20,
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Warn().Err(err).Msg("sqs receive error")
			continue
		}
		for _, msg := range resp.Messages {
			w.processMessage(ctx, msg.Body, msg.ReceiptHandle, out)
		}
	}
}

// s3EventNotification is the JSON envelope for S3 event notifications delivered via SQS.
type s3EventNotification struct {
	Records []struct {
		EventName string `json:"eventName"`
		S3        struct {
			Bucket struct {
				Name string `json:"name"`
			} `json:"bucket"`
			Object struct {
				Key  string `json:"key"`
				Size int64  `json:"size"`
			} `json:"object"`
		} `json:"s3"`
	} `json:"Records"`
}

func (w *SQSWatcher) processMessage(ctx context.Context, body *string, receiptHandle *string, out chan<- ObjectEvent) {
	// Always delete the message — sync is idempotent via Redis state.
	defer func() {
		if receiptHandle == nil {
			return
		}
		if _, err := w.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(w.queueURL),
			ReceiptHandle: receiptHandle,
		}); err != nil && ctx.Err() == nil {
			log.Warn().Err(err).Msg("sqs delete message error")
		}
	}()

	if body == nil {
		return
	}
	var notification s3EventNotification
	if err := json.Unmarshal([]byte(*body), &notification); err != nil {
		log.Warn().Err(err).Msg("sqs message parse error")
		return
	}
	for _, rec := range notification.Records {
		out <- ObjectEvent{
			Bucket:   rec.S3.Bucket.Name,
			Key:      decodeEventKey(rec.S3.Bucket.Name, rec.S3.Object.Key),
			Size:     rec.S3.Object.Size,
			IsDelete: strings.HasPrefix(rec.EventName, "ObjectRemoved"),
		}
	}
}
