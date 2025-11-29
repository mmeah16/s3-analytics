package services

import (
	"context"
	"fmt"
	"mime/multipart"
	"s3-analytics/internal/aws"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

type S3Service struct {
	client *s3.Client 
	bucket string 
}

func NewS3Service(c *aws.S3Client) *S3Service {
	return &S3Service{
		client: c.Client,
		bucket: c.Bucket,
	}
}

func (s *S3Service) UploadFileToS3(ctx context.Context, fh *multipart.FileHeader, traceId string) (string, string, error) {
	file, err := fh.Open()

	if err != nil {
		return "", "", fmt.Errorf("failed to open uploaded file: %w", err)
	}

	defer file.Close()

	id := uuid.New().String()
	key := fmt.Sprintf("raw/%s-%s", id, fh.Filename)


	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key: &key,
		Body: file,
		Metadata: map[string]string{
			"trace_id" : traceId,
		},
	})

	if err != nil {
		return "", "", fmt.Errorf("s3 upload failed: %w", err)
	}

	return key, id, nil
}