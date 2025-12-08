package services

import (
	"context"
	"fmt"
	"log/slog"
	"s3-analytics/internal/aws"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/jsii-runtime-go"
)

type CloudWatchService struct {
	client *cloudwatch.Client 
}

func NewCloudWatchService(cw *aws.CloudWatchClient) *CloudWatchService {
	return &CloudWatchService{
		client: cw.Client,
	}
}

func (cw *CloudWatchService) PutMetrics(ctx context.Context, endpoint string, latency int) error {
	_, err := cw.client.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
		Namespace: jsii.String("FilePipeline/API"),
		MetricData: []types.MetricDatum{
			{
				MetricName: jsii.String("RequestsCount"),
				Unit: types.StandardUnitCount,
				Value: jsii.Number(1),
				Dimensions: []types.Dimension{
					{Name: jsii.String("Endpoint"), Value: jsii.String(endpoint)},
				},
			},
			{
				MetricName: jsii.String("RequestLatencyMs"),
				Unit: types.StandardUnitMilliseconds,
				Value: jsii.Number(float64(latency)),
				Dimensions: []types.Dimension{
					{Name: jsii.String("Endpoint"), Value: jsii.String(endpoint)},
				},
			},
		},
	})

	if err != nil {
		return fmt.Errorf("failed to add metrics: %w", err)
	}
	return nil
}

func (cw *CloudWatchService) PutFailureMetric(ctx context.Context, endpoint string) error {
    _, err := cw.client.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
        Namespace: jsii.String("FilePipeline/API"),
        MetricData: []types.MetricDatum{
            {
                MetricName: jsii.String("RequestFailures"),
                Unit: types.StandardUnitCount,
                Value: jsii.Number(1),
                Dimensions: []types.Dimension{
                    {Name: jsii.String("Endpoint"), Value: jsii.String(endpoint)},
                },
            },
        },
    })
	if err != nil {
		return fmt.Errorf("failed to add metrics: %w", err)
	}
	return nil
}

func (cw *CloudWatchService) EmitAsyncMetrics(
    ctx context.Context,
    endpoint string,
    latency int,
    log *slog.Logger,
) {
    go func() {
        // Protect against panics inside the goroutine
        defer func() {
            if r := recover(); r != nil {
                log.Error("panic in metrics goroutine", "panic", r)
            }
        }()

        // Emit success metrics
        if err := cw.PutMetrics(ctx, endpoint, latency); err != nil {
            log.Error("Failed to publish metrics", "error", err)
        }
    }()
}

func (cw *CloudWatchService) EmitAsyncFailure(
    ctx context.Context,
    endpoint string,
    log *slog.Logger,
) {
    go func() {
        defer func() {
            if r := recover(); r != nil {
                log.Error("panic in metrics goroutine", "panic", r)
            }
        }()

        if err := cw.PutFailureMetric(ctx, endpoint); err != nil {
            log.Error("Failed to publish failure metric", "error", err)
        }
    }()
}