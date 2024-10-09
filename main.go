package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func main() {
	// Parse input arguments
	redisURL := os.Getenv("REDIS_URL")           // Redis DB URL
	streams := os.Getenv("REDIS_STREAMS")        // Comma-separated list of streams
	consumerGroup := os.Getenv("CONSUMER_GROUP") // Consumer group name
	lambdaName := os.Getenv("LAMBDA_NAME")       // AWS Lambda function name

	// Initialize Redis client
	rdb := initializeRedisClient(redisURL)
	defer rdb.Close()

	// Ensure consumer group exists
	streamList := splitStreams(streams)
	for _, stream := range streamList {
		ensureConsumerGroupExists(rdb, stream, consumerGroup)
	}

	// AWS Lambda setup
	sess := session.Must(session.NewSession())
	lambdaClient := lambda.New(sess)

	// Consume stream messages
	for {
		entries, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    consumerGroup,
			Consumer: "consumer-1", // You can replace with a dynamic consumer name
			Streams:  append(streamList, ">"),
			Count:    10,
			Block:    0,
		}).Result()

		if err != nil {
			log.Printf("Error reading from Redis stream: %v", err)
			continue
		}

		// Process each entry and invoke AWS Lambda
		for _, entry := range entries {
			for _, message := range entry.Messages {
				log.Printf("Processing message ID: %s from stream: %s", message.ID, entry.Stream)

				err = invokeLambda(lambdaClient, lambdaName, message)
				if err != nil {
					log.Printf("Error invoking Lambda: %v", err)
				} else {
					// Acknowledge message
					rdb.XAck(ctx, entry.Stream, consumerGroup, message.ID)
					log.Printf("Message ID %s acknowledged in consumer group %s", message.ID, consumerGroup)
				}
			}
		}
	}
}

// initializeRedisClient initializes the Redis client with connection pool settings
func initializeRedisClient(redisURL string) *redis.Client {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("Failed to parse Redis URL: %v", err)
	}

	rdb := redis.NewClient(opts)
	return rdb
}

// Split streams by comma
func splitStreams(streams string) []string {
	return strings.Split(streams, ",")
}

// ensureConsumerGroupExists checks if the consumer group exists and creates it if necessary
func ensureConsumerGroupExists(rdb *redis.Client, stream string, consumerGroup string) {
	// Try to create the consumer group
	err := rdb.XGroupCreateMkStream(ctx, stream, consumerGroup, "$").Err()
	if err != nil {
		if strings.Contains(err.Error(), "BUSYGROUP Consumer Group name already exists") {
			// If the group already exists, log a message and continue
			log.Printf("Consumer group %s already exists for stream %s, skipping creation.", consumerGroup, stream)
		} else {
			log.Fatalf("Error creating consumer group: %v", err)
		}
	} else {
		log.Printf("Consumer group %s created for stream %s.", consumerGroup, stream)
	}
}

// Invoke the AWS Lambda function
func invokeLambda(lambdaClient *lambda.Lambda, functionName string, message redis.XMessage) error {
	payload := []byte(fmt.Sprintf(`{"message": "%v"}`, message.Values))
	result, err := lambdaClient.Invoke(&lambda.InvokeInput{
		FunctionName: aws.String(functionName),
		Payload:      payload,
	})
	if err != nil {
		return err
	}

	// Log the Lambda response
	log.Printf("Lambda invoked successfully. StatusCode: %d, Payload: %s", *result.StatusCode, string(result.Payload))
	return nil
}
