package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/dlancioni/backend-challenge-go/internal/config"
)

type API interface {
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, opts ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	SendMessage(ctx context.Context, in *sqs.SendMessageInput, opts ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	GetQueueUrl(ctx context.Context, in *sqs.GetQueueUrlInput, opts ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
	GetQueueAttributes(ctx context.Context, in *sqs.GetQueueAttributesInput, opts ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
	SetQueueAttributes(ctx context.Context, in *sqs.SetQueueAttributesInput, opts ...func(*sqs.Options)) (*sqs.SetQueueAttributesOutput, error)
	CreateQueue(ctx context.Context, in *sqs.CreateQueueInput, opts ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error)
	PurgeQueue(ctx context.Context, in *sqs.PurgeQueueInput, opts ...func(*sqs.Options)) (*sqs.PurgeQueueOutput, error)
}

var _ API = (*sqs.Client)(nil)

func NewClient(ctx context.Context, cfg config.AWSConfig) (*sqs.Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(cfg.Region)}
	if cfg.Endpoint != "" && os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	return sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	}), nil
}

type QueueURLs struct {
	Input    string
	InputDLQ string
	Events   string
}

func NormalizeQueueURL(queueURL, endpoint string) string {
	if endpoint == "" {
		return queueURL
	}
	q, err := url.Parse(queueURL)
	if err != nil {
		return queueURL
	}
	e, err := url.Parse(endpoint)
	if err != nil || e.Host == "" {
		return queueURL
	}
	q.Scheme, q.Host = e.Scheme, e.Host
	return q.String()
}

func ResolveQueues(ctx context.Context, api API, endpoint string, q config.QueuesConfig, needInput, needEvents bool) (QueueURLs, error) {
	var urls QueueURLs
	lookup := func(name string, dst *string) error {
		out, err := api.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
		if err != nil {
			return fmt.Errorf("resolve queue %s: %w", name, err)
		}
		*dst = NormalizeQueueURL(aws.ToString(out.QueueUrl), endpoint)
		return nil
	}
	if needInput {
		if err := lookup(q.Input, &urls.Input); err != nil {
			return urls, err
		}
		if err := lookup(q.InputDLQ, &urls.InputDLQ); err != nil {
			return urls, err
		}
	}
	if needEvents {
		if err := lookup(q.Events, &urls.Events); err != nil {
			return urls, err
		}
	}
	return urls, nil
}

func EnsureQueues(ctx context.Context, api API, endpoint string, q config.QueuesConfig) (QueueURLs, error) {
	fifo := func(extra map[string]string) map[string]string {
		attrs := map[string]string{
			string(types.QueueAttributeNameFifoQueue):                 "true",
			string(types.QueueAttributeNameContentBasedDeduplication): "false",
			string(types.QueueAttributeNameMessageRetentionPeriod):    strconv.Itoa(int((14 * 24 * time.Hour).Seconds())),
		}
		for k, v := range extra {
			attrs[k] = v
		}
		return attrs
	}
	create := func(name string, attrs map[string]string) (url, arn string, err error) {
		out, err := api.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attrs})
		if err != nil {
			return "", "", fmt.Errorf("create queue %s: %w", name, err)
		}
		url = NormalizeQueueURL(aws.ToString(out.QueueUrl), endpoint)
		attr, err := api.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(url), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
		if err != nil {
			return "", "", fmt.Errorf("get arn of %s: %w", name, err)
		}
		return url, attr.Attributes[string(types.QueueAttributeNameQueueArn)], nil
	}
	setPolicy := func(url string, policy map[string]any) error {
		raw, _ := json.Marshal(policy)
		_, err := api.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
			QueueUrl: aws.String(url), Attributes: map[string]string{string(types.QueueAttributeNamePolicy): string(raw)}})
		return err
	}
	statement := func(sid, principal string, actions []string, arn string) map[string]any {
		return map[string]any{"Sid": sid, "Effect": "Allow", "Principal": map[string]string{"AWS": principal},
			"Action": actions, "Resource": arn}
	}
	policy := func(stmts ...map[string]any) map[string]any {
		return map[string]any{"Version": "2012-10-17", "Statement": stmts}
	}
	consume := []string{"sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"}

	var urls QueueURLs
	dlqURL, dlqARN, err := create(q.InputDLQ, fifo(map[string]string{}))
	if err != nil {
		return urls, err
	}
	if err := setPolicy(dlqURL, policy(statement("ServiceMayDeadLetter", q.ServicePrincipal,
		[]string{"sqs:SendMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"}, dlqARN))); err != nil {
		return urls, err
	}
	redrive, _ := json.Marshal(map[string]any{"deadLetterTargetArn": dlqARN, "maxReceiveCount": q.MaxReceiveCount})
	inURL, inARN, err := create(q.Input, fifo(map[string]string{
		string(types.QueueAttributeNameVisibilityTimeout): strconv.Itoa(int(q.VisibilityTimeout.Seconds())),
		string(types.QueueAttributeNameRedrivePolicy):     string(redrive),
	}))
	if err != nil {
		return urls, err
	}
	if err := setPolicy(inURL, policy(
		statement("ProducersMaySend", q.ProducerPrincipal, []string{"sqs:SendMessage", "sqs:GetQueueUrl"}, inARN),
		statement("ServiceMayConsume", q.ServicePrincipal, consume, inARN))); err != nil {
		return urls, err
	}
	evURL, evARN, err := create(q.Events, fifo(map[string]string{
		string(types.QueueAttributeNameVisibilityTimeout): strconv.Itoa(int(q.VisibilityTimeout.Seconds())),
	}))
	if err != nil {
		return urls, err
	}
	if err := setPolicy(evURL, policy(
		statement("ServiceMayPublish", q.ServicePrincipal, []string{"sqs:SendMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"}, evARN),
		statement("EventConsumersMayConsume", q.EventConsumerPrincipal, consume, evARN))); err != nil {
		return urls, err
	}
	urls = QueueURLs{Input: inURL, InputDLQ: dlqURL, Events: evURL}
	return urls, nil
}

type QueueChecker struct {
	API  API
	URLs []string
}

func (c *QueueChecker) Name() string { return "sqs" }

func (c *QueueChecker) Check(ctx context.Context) error {
	if len(c.URLs) == 0 {
		return errors.New("no queue configured")
	}
	for _, u := range c.URLs {
		if _, err := c.API.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(u), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}}); err != nil {
			return err
		}
	}
	return nil
}
