package services

import (
	"context"
	"fmt"
	"log"
	"s3-analytics/internal/aws"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type DynamoDBService struct {
	client *dynamodb.Client
	tableName string
}

type FileMetadata struct {
    ID              string    `dynamodbav:"id"`
    Filename        string    `dynamodbav:"filename"`
    Size            int64     `dynamodbav:"size"`
    ProcessingState string    `dynamodbav:"processingState"`
    CreatedAt       time.Time `dynamodbav:"createdAt"`
    Sha256          string    `dynamodbav:"sha256"`
    ProcessedKey    string    `dynamodbav:"processedKey"`
}

func NewDynamoDBService(d *aws.DynamoDBClient) *DynamoDBService {
	return &DynamoDBService{
		client: d.Client,
		tableName: d.TableName,
	}
}

func (d *DynamoDBService) GetAllItems(ctx context.Context) (*dynamodb.ScanOutput, error) {

	res, err := d.client.Scan(ctx, &dynamodb.ScanInput{
		TableName: &d.tableName,
	})

	if err != nil {
		return nil, fmt.Errorf("dynamodb scan failed: %w", err)
	}

	return res, nil 
}

func (d *DynamoDBService) CreateItem(ctx context.Context, metadata *FileMetadata) (*dynamodb.PutItemOutput, error)  {
	
	item, err := attributevalue.MarshalMap(metadata)
	
	if err != nil {
		return nil, fmt.Errorf("Got error marshalling new movie item: %s", err)
	}

	res, err := d.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &d.tableName,
		Item: item,
	})

	if err != nil {
		return nil, fmt.Errorf("dynamodb PutItem failed: %w", err)
	}

	return res, nil
}

func (d *DynamoDBService) GetFileById(ctx context.Context, id string) (FileMetadata, error) {
	fileMetadata := FileMetadata{ID: id}
	response, err := d.client.GetItem(ctx, &dynamodb.GetItemInput{
		Key: fileMetadata.GetKey(), 
		TableName: &d.tableName,
	})
	if err != nil {
		log.Printf("Couldn't get info about file %v. Here's why: %v\n", id, err)
	} else {
		err = attributevalue.UnmarshalMap(response.Item, &fileMetadata)
		if err != nil {
			log.Printf("Couldn't unmarshal response. Here's why: %v\n", err)
		}
	}
	return fileMetadata, err
}

func (fm FileMetadata) GetKey() map[string]types.AttributeValue {
	id, err := attributevalue.Marshal(fm.ID)
	if err != nil {
		panic(err)
	}
	return map[string]types.AttributeValue{"id": id}
}
