package main

import (
	"fmt"
	"os"
	"path"

	"channel-pruner/configs"
	"channel-pruner/purger"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/lbryio/lbry.go/v2/stream"
	"github.com/nikooo777/lbry-blobs-downloader/chainquery"
	"github.com/nikooo777/lbry-blobs-downloader/http"
	"github.com/nikooo777/lbry-blobs-downloader/shared"
	"github.com/sirupsen/logrus"
)

func main() {
	if len(os.Args) < 2 {
		logrus.Fatalf("Usage: %s <channel-claim-id>", os.Args[0])
	}
	channelClaimId := os.Args[1]
	streams, err := chainquery.GetChannelStreams(channelClaimId)
	if err != nil {
		logrus.Fatalf("Failed to get channel streams: %s", err)
	}
	err = configs.Init("./config.json")
	if err != nil {
		logrus.Fatalf(err.Error())
	}
	pruner, err := purger.Init(configs.Configuration.S3)
	if err != nil {
		panic(err)
	}
	downloadServer := "blobcache-eu.odycdn.com"
	HTTPPort := 5569
	shared.ReflectorHttpServer = fmt.Sprintf("%s:%d", downloadServer, HTTPPort)

	for _, s := range streams {
		sdBlob, err := http.DownloadBlob(s.SdHash, false, nil)
		if err != nil {
			logrus.Fatalf("Failed to download blob: %s", err)
		}
		sdb := &stream.SDBlob{}
		err = sdb.FromBlob(*sdBlob)
		hashes := shared.GetStreamHashes(sdb)
		if err != nil {
			logrus.Fatalf("Failed to create file: %s", err)
		}
		hashes = append(hashes, s.SdHash)
		WriteToFile(hashes, fmt.Sprintf("./%s.txt", s.ClaimId), channelClaimId)
		err = deleteObjects(hashes, pruner)
		if err != nil {
			logrus.Fatalf("error while processing stream %s: %s", s.ClaimId, err)
		}
	}

	logrus.Infof("dumped %d streams", len(streams))
}

func WriteToFile(hashes []string, filename, subfolder string) {
	err := os.MkdirAll(subfolder, 0700)
	if err != nil {
		logrus.Fatalf(err.Error())
	}
	f, err := os.Create(path.Join(subfolder, filename))
	if err != nil {
		logrus.Fatalf("Failed to create file: %s", err)
	}
	defer f.Close()
	for _, h := range hashes {
		_, err = f.WriteString(fmt.Sprintf("%s\n", h))
		if err != nil {
			logrus.Fatalf("Failed to write to file: %s", err)
		}
	}
}

func deleteObjects(hashes []string, p *purger.Purger) error {
	// Map to track the status of each object's deletion
	objectsDeleteStatus := make(map[string]bool)
	for _, h := range hashes {
		objectsDeleteStatus[h] = false
	}

	// Batch size limit for AWS S3 delete operation
	batchSize := 1000

	// Delete objects in batches
	for i := 0; i < len(hashes); i += batchSize {
		end := i + batchSize
		if end > len(hashes) {
			end = len(hashes)
		}
		// Prepare delete input for the current batch
		delInput := &s3.Delete{
			Objects: make([]*s3.ObjectIdentifier, 0, end-i),
		}
		for _, h := range hashes[i:end] {
			delInput.Objects = append(delInput.Objects, &s3.ObjectIdentifier{Key: aws.String(h)})
		}

		// Perform the delete operation
		deletedKeys, err := p.DeleteObjects(delInput)
		if err != nil {
			return err
		}

		// Update the deletion status for successfully deleted objects
		for _, d := range deletedKeys {
			objectsDeleteStatus[d] = true
		}
	}

	// Log any objects that failed to delete
	failuresCount := 0
	for k, v := range objectsDeleteStatus {
		if !v {
			logrus.Warnf("Failed to delete object: %s", k)
			failuresCount++
		}
	}

	if failuresCount > 0 {
		return fmt.Errorf("failed to delete %d objects", failuresCount)
	}
	return nil
}
