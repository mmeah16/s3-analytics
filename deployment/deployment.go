package main

import (
	"os"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsdynamodb"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsevents"
	targets "github.com/aws/aws-cdk-go/awscdk/v2/awseventstargets"
	"github.com/aws/aws-cdk-go/awscdk/v2/awslambda"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3"
	"github.com/joho/godotenv"

	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

type DeploymentStackProps struct {
	awscdk.StackProps
	TableName string
	BucketName string
}

func NewDeploymentStack(scope constructs.Construct, id string, props *DeploymentStackProps) awscdk.Stack {
	var sprops awscdk.StackProps
	if props != nil {
		sprops = props.StackProps
	}
	stack := awscdk.NewStack(scope, &id, &sprops)

	if props.BucketName == "" {
		panic("BucketName must be provided")
	}
	if props.TableName == "" {
		panic("TableName must be provided")
	}

	// The code that defines your stack goes here

	// Create S3 bucket
	bucket := awss3.NewBucket(stack, jsii.String("FileBucket"), &awss3.BucketProps{
		BucketName: jsii.String(props.BucketName),
		BlockPublicAccess: awss3.BlockPublicAccess_BLOCK_ALL(),
		EventBridgeEnabled: jsii.Bool(true),
	})

	// Create DynamoDB Table 
	table := awsdynamodb.NewTableV2(stack, jsii.String("FileMetadataTable"), &awsdynamodb.TablePropsV2{
		TableName:   jsii.String(props.TableName),
		PartitionKey: &awsdynamodb.Attribute{
			Name: jsii.String("id"),
			Type: awsdynamodb.AttributeType_STRING,
		},
	})

	table.AddGlobalSecondaryIndex(&awsdynamodb.GlobalSecondaryIndexPropsV2{
		IndexName: jsii.String("Sha256Index"),
		PartitionKey: &awsdynamodb.Attribute{
			Name: jsii.String("sha256"),
			Type: awsdynamodb.AttributeType_STRING,
		},
	})

	// Lambda processor
	lambda := awslambda.NewFunction(stack, jsii.String("ProcessLambda"), &awslambda.FunctionProps{
		FunctionName: jsii.String("ProcessLambda"),
		Runtime: awslambda.Runtime_PYTHON_3_12(),
		Handler: jsii.String("processor.lambda_handler"),
		Code:    awslambda.Code_FromAsset(jsii.String("../lambdas/file_processor"), nil),
		Timeout: awscdk.Duration_Seconds(jsii.Number(120)),
		Environment: &map[string]*string{
			"TABLE_NAME": table.TableName(),
			"BUCKET_NAME": bucket.BucketName(),
		},
	})

	table.GrantReadWriteData(lambda)
	bucket.GrantReadWrite(lambda, nil)

	// Eventbridge trigger on S3 uploads
	rule := awsevents.NewRule(stack, jsii.String("RawFileUploadRule"), &awsevents.RuleProps{
		RuleName: jsii.String("RawFileUploadRule"),
		EventPattern: &awsevents.EventPattern{
			Source:     &[]*string{jsii.String("aws.s3")},
			DetailType: &[]*string{jsii.String("Object Created")},
			Detail: &map[string]interface{}{
				"bucket": map[string]interface{}{
					"name": []interface{}{
						*bucket.BucketName(),
					},
				},
				"object": map[string]interface{}{
					"key": []interface{}{
						map[string]interface{}{
							"prefix": "raw/",
						},
					},
				},
			},
		},
	})
	rule.AddTarget(targets.NewLambdaFunction(lambda, nil))

	return stack
}

func main() {
	defer jsii.Close()

	app := awscdk.NewApp(nil)

	NewDeploymentStack(app, "DeploymentStack", &DeploymentStackProps{
		StackProps: awscdk.StackProps{
			Env: env(),
		},
		BucketName: os.Getenv("BUCKET_NAME"),
		TableName: os.Getenv("TABLE_NAME"),
	})

	app.Synth(nil)
}

func env() *awscdk.Environment {
	err := godotenv.Load()

	if err != nil {
		panic("Could not retrieve environment variables.")
	}

	return &awscdk.Environment{
		Account: jsii.String(os.Getenv("AWS_ACCOUNT_ID")),
		Region:  jsii.String(os.Getenv("REGION")),
	}
}
