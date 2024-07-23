package purger

import (
	"channel-pruner/configs"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/lbryio/lbry.go/v2/extras/errors"
)

type Purger struct {
	session *session.Session
	client  *s3.S3
	bucket  string
}

func Init(awsCreds configs.AWSS3Config) (*Purger, error) {
	creds := credentials.NewStaticCredentials(awsCreds.AccessKey, awsCreds.SecretKey, "")
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String(awsCreds.Region),
		Credentials:      creds,
		Endpoint:         aws.String(awsCreds.Endpoint),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		return nil, errors.Err(err)
	}
	// Create S3 service client
	svc := s3.New(sess)

	return &Purger{
		session: sess,
		client:  svc,
		bucket:  awsCreds.Bucket,
	}, nil
}
func (p *Purger) DeleteObjects(delInput *s3.Delete) ([]string, error) {
	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(p.bucket),
		Delete: delInput,
	}

	resp, err := p.client.DeleteObjects(input)
	if err != nil {
		return nil, err
	}

	var deletedKeys []string
	//for _, o := range delInput.Objects {
	//	deletedKeys = append(deletedKeys, *o.Key)
	//}
	for _, deleted := range resp.Deleted {
		deletedKeys = append(deletedKeys, *deleted.Key)
	}

	return deletedKeys, nil
}
