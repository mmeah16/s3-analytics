package aws

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Client struct {
	Client *s3.Client
	Bucket string 
}

func NewS3Client(ctx context.Context, bucket string) *S3Client {
	cfg, err := config.LoadDefaultConfig(ctx)

	if err != nil {
		log.Fatalf("Unable to load AWS config: %v", err)
	}
	
	client := s3.NewFromConfig(cfg)
	
	return &S3Client {
		Client: client,
		Bucket: bucket,
	}
}