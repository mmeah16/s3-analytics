package services

import (
	"context"
	"fmt"
	"s3-analytics/internal/aws"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type DynamoDBService struct {
	client *dynamodb.Client
	tableName string
}

type FileMetadata struct {
    ID        string
    Filename  string
    Size      int64
    Status    string // uploaded | processing | done
    CreatedAt time.Time
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

func (d *DynamoDBService) CreateItem(ctx context.Context, metadata *FileMetadata) (*dynamodb.PutItemOutput, error)  {
	
	av, err := attributevalue.MarshalMap(metadata)
	
	if err != nil {
		return nil, fmt.Errorf("Got error marshalling new movie item: %s", err)
	}

	res, err := d.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &d.tableName,
		Item: av,
	})

	if err != nil {
		return nil, fmt.Errorf("dynamodb PutItem failed: %w", err)
	}

	return res, nil
}