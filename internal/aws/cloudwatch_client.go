package aws

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
)

type CloudWatchClient struct {
	Client *cloudwatch.Client
}

func NewCloudWatchClient(ctx context.Context) *CloudWatchClient{
	cfg, err := config.LoadDefaultConfig(ctx)

	if err != nil {
		log.Fatalf("Unable to load AWS config: %v", err)
	}

	client := cloudwatch.NewFromConfig(cfg)

	return &CloudWatchClient{
		Client: client,
	}
}