package watcher

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// fakeSQSClient implements sqsClient for testing.
// It delivers messages on the first ReceiveMessage call, then blocks until ctx is done.
type fakeSQSClient struct {
	messages  []types.Message
	callCount int
	deleted   []string
}

func (f *fakeSQSClient) ReceiveMessage(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	f.callCount++
	if f.callCount > 1 {
		<-ctx.Done()
		return &sqs.ReceiveMessageOutput{}, nil
	}
	return &sqs.ReceiveMessageOutput{Messages: f.messages}, nil
}

func (f *fakeSQSClient) DeleteMessage(_ context.Context, params *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	if params.ReceiptHandle != nil {
		f.deleted = append(f.deleted, *params.ReceiptHandle)
	}
	return &sqs.DeleteMessageOutput{}, nil
}

func sqsBody(bucket, key string, size int64) string {
	return sqsBodyWithEvent(bucket, key, size, "s3:ObjectCreated:Put")
}

func sqsBodyWithEvent(bucket, key string, size int64, eventName string) string {
	type obj struct {
		Key  string `json:"key"`
		Size int64  `json:"size"`
	}
	type bkt struct {
		Name string `json:"name"`
	}
	type s3rec struct {
		Bucket bkt `json:"bucket"`
		Object obj `json:"object"`
	}
	type record struct {
		EventName string `json:"eventName"`
		S3        s3rec  `json:"s3"`
	}
	type envelope struct {
		Records []record `json:"Records"`
	}
	b, _ := json.Marshal(envelope{Records: []record{{
		EventName: eventName,
		S3:        s3rec{Bucket: bkt{Name: bucket}, Object: obj{Key: key, Size: size}},
	}}})
	return string(b)
}

func TestNewSQS(t *testing.T) {
	tests := []struct {
		name    string
		cfg     SQSConfig
		wantErr bool
	}{
		{
			name:    "with_static_credentials",
			cfg:     SQSConfig{QueueURL: "https://sqs.us-east-1.amazonaws.com/123/q", Region: "us-east-1", AccessKey: "key", SecretKey: "secret"},
			wantErr: false,
		},
		{
			name:    "without_credentials_uses_defaults",
			cfg:     SQSConfig{QueueURL: "https://sqs.us-east-1.amazonaws.com/123/q", Region: "us-east-1"},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w, err := NewSQS(tc.cfg)
			if tc.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tc.wantErr && w == nil {
				t.Error("expected non-nil watcher")
			}
		})
	}
}

// errorSQSClient returns an error on the first ReceiveMessage, then blocks.
type errorSQSClient struct {
	callCount int
}

func (f *errorSQSClient) ReceiveMessage(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	f.callCount++
	if f.callCount == 1 {
		return nil, errors.New("transient error")
	}
	<-ctx.Done()
	return &sqs.ReceiveMessageOutput{}, nil
}

func (f *errorSQSClient) DeleteMessage(_ context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	return &sqs.DeleteMessageOutput{}, nil
}

func TestSQSWatcherPollError(t *testing.T) {
	w := &SQSWatcher{client: &errorSQSClient{}, queueURL: "https://sqs.test/queue"}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	ch, err := w.Watch(ctx, nil)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	// Channel closes when ctx expires; no events expected.
	for range ch {
		t.Error("unexpected event from error client")
	}
}

func TestSQSWatcher(t *testing.T) {
	tests := []struct {
		name         string
		messages     []types.Message
		wantEvents   []ObjectEvent
		wantDeletedN int
	}{
		{
			name: "encoded key is decoded",
			messages: []types.Message{
				{Body: aws.String(sqsBody("my-bucket", "dir/a+b%2Bc.txt", 7)), ReceiptHandle: aws.String("rh1")},
			},
			wantEvents:   []ObjectEvent{{Bucket: "my-bucket", Key: "dir/a b+c.txt", Size: 7}},
			wantDeletedN: 1,
		},
		{
			name: "valid event emitted and message deleted",
			messages: []types.Message{
				{Body: aws.String(sqsBody("my-bucket", "dir/file.txt", 1024)), ReceiptHandle: aws.String("rh1")},
			},
			wantEvents:   []ObjectEvent{{Bucket: "my-bucket", Key: "dir/file.txt", Size: 1024}},
			wantDeletedN: 1,
		},
		{
			name: "malformed json skipped but message deleted",
			messages: []types.Message{
				{Body: aws.String("not-json"), ReceiptHandle: aws.String("rh2")},
			},
			wantEvents:   nil,
			wantDeletedN: 1,
		},
		{
			name: "nil body skipped but message deleted",
			messages: []types.Message{
				// Nil body can't be parsed; we still delete to avoid stuck messages.
				{Body: nil, ReceiptHandle: aws.String("rh3")},
			},
			wantEvents:   nil,
			wantDeletedN: 1,
		},
		{
			name: "multiple records in one message",
			messages: []types.Message{{
				ReceiptHandle: aws.String("rh4"),
				Body: func() *string {
					type obj struct {
						Key  string `json:"key"`
						Size int64  `json:"size"`
					}
					type bkt struct {
						Name string `json:"name"`
					}
					type s3rec struct {
						Bucket bkt `json:"bucket"`
						Object obj `json:"object"`
					}
					type record struct {
						EventName string `json:"eventName"`
						S3        s3rec  `json:"s3"`
					}
					type envelope struct {
						Records []record `json:"Records"`
					}
					b, _ := json.Marshal(envelope{Records: []record{
						{EventName: "s3:ObjectCreated:Put", S3: s3rec{Bucket: bkt{Name: "b"}, Object: obj{Key: "k1", Size: 10}}},
						{EventName: "s3:ObjectCreated:Put", S3: s3rec{Bucket: bkt{Name: "b"}, Object: obj{Key: "k2", Size: 20}}},
					}})
					s := string(b)
					return &s
				}(),
			}},
			wantEvents:   []ObjectEvent{{Bucket: "b", Key: "k1", Size: 10}, {Bucket: "b", Key: "k2", Size: 20}},
			wantDeletedN: 1,
		},
		{
			name: "delete event sets IsDelete and message still deleted",
			messages: []types.Message{
				{Body: aws.String(sqsBodyWithEvent("my-bucket", "dir/file.txt", 0, "ObjectRemoved:Delete")), ReceiptHandle: aws.String("rh5")},
			},
			wantEvents:   []ObjectEvent{{Bucket: "my-bucket", Key: "dir/file.txt", IsDelete: true}},
			wantDeletedN: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeSQSClient{messages: tc.messages}
			w := &SQSWatcher{client: fake, queueURL: "https://sqs.test/queue"}

			// Short timeout: first batch is delivered instantly; after that poll blocks
			// until context is cancelled, keeping total test time under 300 ms.
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			ch, err := w.Watch(ctx, nil)
			if err != nil {
				t.Fatalf("Watch: %v", err)
			}

			var got []ObjectEvent
			for e := range ch {
				got = append(got, e)
			}

			if len(got) != len(tc.wantEvents) {
				t.Fatalf("got %d events, want %d", len(got), len(tc.wantEvents))
			}
			for i, e := range got {
				if e != tc.wantEvents[i] {
					t.Errorf("event[%d] = %+v, want %+v", i, e, tc.wantEvents[i])
				}
			}
			if len(fake.deleted) != tc.wantDeletedN {
				t.Errorf("deleted %d messages, want %d", len(fake.deleted), tc.wantDeletedN)
			}
		})
	}
}
