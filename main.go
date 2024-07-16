package main

import (
	"fmt"
	"os"

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
		WriteToFile(hashes, fmt.Sprintf("./%s.txt", s.ClaimId))
	}

	logrus.Infof("dumped %d streams", len(streams))
}

func WriteToFile(hashes []string, filename string) {
	f, err := os.Create(filename)
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
