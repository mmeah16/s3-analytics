package services

import (
	"context"
	"fmt"
	"s3-analytics/internal/aws"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type DynamoDBService struct {
	client *dynamodb.Client
	tableName string
}

func NewDynamoDBService(d *aws.DynamoDBClient) *DynamoDBService {
	return &DynamoDBService{
		client: d.Client,
		tableName: d.TableName,
	}
}

func (d *DynamoDBService) ReadAllItems(ctx context.Context) (*dynamodb.ScanOutput, error) {

	res, err := d.client.Scan(ctx, &dynamodb.ScanInput{
		TableName: &d.tableName,
	})

	if err != nil {
		return nil, fmt.Errorf("dynamodb scan failed: %w", err)
	}

	return res, nil 
}