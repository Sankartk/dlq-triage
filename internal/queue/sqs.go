package queue

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/Sankartk/dlq-triage/internal/domain"
)

// SQSOptions configures the SQS client.
type SQSOptions struct {
	Region string
	// Endpoint overrides the service endpoint, for local emulators.
	Endpoint string
	// WaitTime is the long-poll duration of Receive. Short polls on real SQS
	// can return nothing even when messages exist, so the default is 1 second.
	WaitTime time.Duration
}

// SQS is a Queue backed by Amazon SQS (or an emulator).
type SQS struct {
	client *sqs.Client
	wait   int32
}

// NewSQS builds a client using the default AWS credential chain.
func NewSQS(ctx context.Context, opts SQSOptions) (*SQS, error) {
	var loadOpts []func(*config.LoadOptions) error
	if opts.Region != "" {
		loadOpts = append(loadOpts, config.WithRegion(opts.Region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	client := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		if opts.Endpoint != "" {
			o.BaseEndpoint = aws.String(opts.Endpoint)
		}
	})
	wait := opts.WaitTime
	if wait <= 0 {
		wait = time.Second
	}
	if wait > 20*time.Second {
		wait = 20 * time.Second
	}
	return &SQS{client: client, wait: int32(wait / time.Second)}, nil
}

func (s *SQS) Receive(ctx context.Context, url string, max int, visibility time.Duration) ([]domain.Message, error) {
	if max < 1 || max > 10 {
		return nil, errors.New("max must be between 1 and 10")
	}
	out, err := s.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:                    aws.String(url),
		MaxNumberOfMessages:         int32(max),
		VisibilityTimeout:           int32(math.Ceil(visibility.Seconds())),
		WaitTimeSeconds:             s.wait,
		MessageAttributeNames:       []string{"All"},
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
	})
	if err != nil {
		return nil, err
	}
	msgs := make([]domain.Message, 0, len(out.Messages))
	for _, m := range out.Messages {
		msgs = append(msgs, fromSQS(m))
	}
	return msgs, nil
}

func fromSQS(m types.Message) domain.Message {
	msg := domain.Message{
		ID:     aws.ToString(m.MessageId),
		Body:   aws.ToString(m.Body),
		Handle: aws.ToString(m.ReceiptHandle),
	}
	if len(m.MessageAttributes) > 0 {
		msg.Attributes = make(map[string]domain.Attribute, len(m.MessageAttributes))
		for name, v := range m.MessageAttributes {
			dt := aws.ToString(v.DataType)
			switch {
			case strings.HasPrefix(dt, "Binary"):
				msg.Attributes[name] = domain.Attribute{DataType: dt, Value: base64.StdEncoding.EncodeToString(v.BinaryValue)}
			default:
				msg.Attributes[name] = domain.Attribute{DataType: dt, Value: aws.ToString(v.StringValue)}
			}
		}
	}
	if v, ok := m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)]; ok {
		msg.ReceiveCount, _ = strconv.Atoi(v)
	}
	if v, ok := m.Attributes[string(types.MessageSystemAttributeNameSentTimestamp)]; ok {
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil {
			msg.SentAt = time.UnixMilli(ms).UTC()
		}
	}
	msg.GroupID = m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
	return msg
}

func (s *SQS) Delete(ctx context.Context, url string, msg domain.Message) error {
	_, err := s.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(url),
		ReceiptHandle: aws.String(msg.Handle),
	})
	return err
}

func (s *SQS) Send(ctx context.Context, url string, msg domain.Message, dedupID string) (string, error) {
	in := &sqs.SendMessageInput{
		QueueUrl:    aws.String(url),
		MessageBody: aws.String(msg.Body),
	}
	if len(msg.Attributes) > 0 {
		attrs := make(map[string]types.MessageAttributeValue, len(msg.Attributes))
		for name, a := range msg.Attributes {
			v := types.MessageAttributeValue{DataType: aws.String(a.DataType)}
			if strings.HasPrefix(a.DataType, "Binary") {
				raw, err := base64.StdEncoding.DecodeString(a.Value)
				if err != nil {
					return "", fmt.Errorf("attribute %q: invalid binary value: %w", name, err)
				}
				v.BinaryValue = raw
			} else {
				v.StringValue = aws.String(a.Value)
			}
			attrs[name] = v
		}
		in.MessageAttributes = attrs
	}
	if strings.HasSuffix(url, ".fifo") {
		group := msg.GroupID
		if group == "" {
			group = "dlq-triage"
		}
		in.MessageGroupId = aws.String(group)
		if dedupID != "" {
			in.MessageDeduplicationId = aws.String(dedupID)
		}
	}
	out, err := s.client.SendMessage(ctx, in)
	if err != nil {
		return "", err
	}
	return aws.ToString(out.MessageId), nil
}

func (s *SQS) Depth(ctx context.Context, url string) (domain.QueueDepth, error) {
	out, err := s.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(url),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			types.QueueAttributeNameApproximateNumberOfMessagesDelayed,
		},
	})
	if err != nil {
		return domain.QueueDepth{}, err
	}
	get := func(n types.QueueAttributeName) int {
		v, _ := strconv.Atoi(out.Attributes[string(n)])
		return v
	}
	return domain.QueueDepth{
		Visible:  get(types.QueueAttributeNameApproximateNumberOfMessages),
		InFlight: get(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible),
		Delayed:  get(types.QueueAttributeNameApproximateNumberOfMessagesDelayed),
	}, nil
}
