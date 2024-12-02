package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/account"
	accountTypes "github.com/aws/aws-sdk-go-v2/service/account/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type BucketClient struct {
	S3Client *s3.Client
}

func (b BucketClient) ListBuckets(ctx context.Context, bucketRegion string, prefix string) ([]types.Bucket, error) {
	var err error
	// var output *s3.ListBucketsOutput
	var buckets []types.Bucket

	bucketPaginator := s3.NewListBucketsPaginator(b.S3Client, &s3.ListBucketsInput{
		BucketRegion: &bucketRegion,
		Prefix:       &prefix,
	})

	for bucketPaginator.HasMorePages() {
		output, err := bucketPaginator.NextPage(ctx)
		if err != nil {
			var apiErr smithy.APIError
			if errors.As(err, &apiErr) && apiErr.ErrorCode() == "AccessDenied" {
				fmt.Println("You don't have permission to list buckets for this account")
				err = apiErr
			} else {
				log.Printf("Couldn't list buckets for your account. Here's why: %+v\n", err)
			}
			break
		} else {
			buckets = append(buckets, output.Buckets...)
		}
	}

	return buckets, err
}

func (b BucketClient) ListObjects(ctx context.Context, bucketName string) ([]types.Object, error) {
	var err error
	// var output *s3.ListObjectsV2Output
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}
	var objects []types.Object

	objectPaginator := s3.NewListObjectsV2Paginator(b.S3Client, input)

	for objectPaginator.HasMorePages() {
		output, err := objectPaginator.NextPage(ctx)
		if err != nil {
			var noBucket *types.NoSuchBucket
			if errors.As(err, &noBucket) {
				log.Printf("Bucket %s does not exist.\n", bucketName)
				err = noBucket
			}
			break
		} else {
			objects = append(objects, output.Contents...)
		}
	}

	return objects, err
}

func (b BucketClient) DeleteObjects(ctx context.Context, bucketName string, objectKeys []string) error {
	var objectIds []types.ObjectIdentifier
	for _, key := range objectKeys {
		objectIds = append(objectIds, types.ObjectIdentifier{Key: aws.String(key)})
	}

	batchSize := 1000
	batches := make([][]types.ObjectIdentifier, 0, (len(objectIds)+batchSize-1)/batchSize)

	for batchSize < len(objectIds) {
		objectIds, batches = objectIds[batchSize:], append(batches, objectIds[0:batchSize:batchSize])
	}
	batches = append(batches, objectIds)

	var err error

	for _, batch := range batches {
		output, err := b.S3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: &types.Delete{Objects: batch, Quiet: aws.Bool(true)},
		})
		if err != nil || len(output.Errors) > 0 {
			log.Printf("Error deleting objects from bucket %s.\n", bucketName)
			if err != nil {
				var noBucket *types.NoSuchBucket
				if errors.As(err, &noBucket) {
					log.Printf("Bucket %s does not exist\n", bucketName)
					err = noBucket
				}
				break
			} else if len(output.Errors) > 0 {
				for _, outErr := range output.Errors {
					log.Printf("%s: %s\n", *outErr.Key, *outErr.Message)
				}
				err = fmt.Errorf("%s", *output.Errors[0].Message)
				break
			} else {
				for _, delObjs := range output.Deleted {
					err = s3.NewObjectNotExistsWaiter(b.S3Client).Wait(
						ctx, &s3.HeadObjectInput{Bucket: aws.String(bucketName), Key: delObjs.Key}, time.Minute)
					if err != nil {
						log.Printf("Failed attempt to wait for object %s to be deleted\n", *delObjs.Key)
					} else {
						log.Printf("Deleted %s.\n", *delObjs.Key)
					}
				}
			}
		}
	}

	return err
}

func (b BucketClient) DeleteBucket(ctx context.Context, bucketName string) error {
	_, err := b.S3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		var noBucket *types.NoSuchBucket
		if errors.As(err, &noBucket) {
			log.Printf("bucket %s does not exist\n", bucketName)
			err = noBucket
		} else {
			log.Printf("Couldn't delete bucket %s because %+v", bucketName, err)
		}
	} else {
		err = s3.NewBucketNotExistsWaiter(b.S3Client).Wait(
			ctx, &s3.HeadBucketInput{Bucket: aws.String(bucketName)}, time.Minute)
		if err != nil {
			log.Printf("Failed attempt to wait for bucket %s to be deleted\n", bucketName)
		} else {
			log.Printf("Deleted %s\n", bucketName)
		}
	}

	return err
}

func main() {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Printf("error getting config\n")
	}

	accountClient := account.NewFromConfig(cfg)
	regions, err := accountClient.ListRegions(context.TODO(), &account.ListRegionsInput{
		RegionOptStatusContains: accountTypes.RegionOptStatusEnabled.Values(),
	})
	if err != nil {
		log.Fatalf("error listing regions: %+v\n", err)
	}

	for _, region := range regions.Regions {
		cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(*region.RegionName))
		if err != nil {
			log.Fatalf("error getting config for region %s: %+v\n", *region.RegionName, err)
		}

		s3Client := s3.NewFromConfig(cfg)
		bucket := BucketClient{S3Client: s3Client}

		buckets, err := bucket.ListBuckets(context.TODO(), cfg.Region, "")
		if err != nil {
			log.Fatalf("error listing buckets: %+v\n", err)
		}

		log.Printf("got %d buckets\n", len(buckets))

		for _, gotBucket := range buckets {
			objects, err := bucket.ListObjects(context.TODO(), *gotBucket.Name)
			if err != nil {
				log.Fatalf("error listing objects for %s: %+v\n", *gotBucket.Name, err)
			}

			var objKeys []string
			for _, object := range objects {
				objKeys = append(objKeys, *object.Key)
			}

			if len(objKeys) != 0 {
				log.Printf("deleting objects in %s\n", *gotBucket.Name)
				err = bucket.DeleteObjects(context.TODO(), *gotBucket.Name, objKeys)
				if err != nil {
					log.Fatalf("error deleting objects: %+v\n", err)
				}

				log.Printf("deleting bucket %s\n", *gotBucket.Name)
				err = bucket.DeleteBucket(context.TODO(), *gotBucket.Name)
				if err != nil {
					log.Fatalf("error deleting bucket %s: %+v\n", *gotBucket.Name, err)
				} else {
					log.Printf("deleted bucket %s\n", *gotBucket.Name)
				}
			} else {
				log.Printf("deleting bucket %s\n", *gotBucket.Name)
				err = bucket.DeleteBucket(context.TODO(), *gotBucket.Name)
				if err != nil {
					log.Fatalf("error deleting bucket %s: %+v\n", *gotBucket.Name, err)
				} else {
					log.Printf("deleted bucket %s\n", *gotBucket.Name)
				}
			}
		}
	}
}
