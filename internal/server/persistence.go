package server

import (
	"compress/gzip"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tidwall/tile38/core"
	"github.com/tidwall/tile38/internal/log"
	metric "github.com/tidwall/tile38/internal/metrics"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	compressedFileSuffix     = ".gz"
	compressedFileType       = ".aof.gz"
	uploadAOFMetricLabel     = "aof_upload"
	getAOFListMetricLabel    = "aof_get_list"
	downloadAOFMetricLabel   = "aof_download"
	compressAOFMetricLabel   = "aof_compress"
	extractAOFMetricLabel    = "aof_extract"
	AOFUploadCronMetricLabel = "aof_upload_cron"
	setupAOFMetricLabel      = "aof_setup"
)

func (server *Server) executeAOFUploadCron() {
	metric.RequestCounter.WithLabelValues(AOFUploadCronMetricLabel).Inc()
	timer := prometheus.NewTimer(metric.JobLatency.WithLabelValues(AOFUploadCronMetricLabel, ""))
	defer timer.ObserveDuration()
	err := server.compressAOF()
	if err != nil {
		metric.FailureCounter.WithLabelValues(AOFUploadCronMetricLabel, "").Inc()
		return
	}
	err = server.uploadAOF()
	if err != nil {
		metric.FailureCounter.WithLabelValues(AOFUploadCronMetricLabel, "").Inc()
		return
	}
	return
}

func (server *Server) compressAOF() error {
	metric.RequestCounter.WithLabelValues(compressAOFMetricLabel).Inc()
	timer := prometheus.NewTimer(metric.JobLatency.WithLabelValues(compressAOFMetricLabel, ""))
	defer timer.ObserveDuration()
	source := core.AppendFileName
	target := source + compressedFileSuffix
	reader, err := os.Open(source)
	if err != nil {
		log.Errorf("Error while opening AOF to compress. Source Location: %v. Error: %+v", source, err)
		metric.FailureCounter.WithLabelValues(compressAOFMetricLabel, "").Inc()
		return err
	}

	filename := filepath.Base(source)
	writer, err := os.Create(target)
	if err != nil {
		log.Errorf("Error while creating compressed AOF. Target Location: %v. Error: %+v", target, err)
		metric.FailureCounter.WithLabelValues(compressAOFMetricLabel, "").Inc()
		return err
	}
	defer writer.Close()

	archiver := gzip.NewWriter(writer)
	archiver.Name = filename
	defer archiver.Close()

	_, err = io.Copy(archiver, reader)
	if err != nil {
		log.Errorf("Error while copying AOF to compressed AOF. Source: %v, Target: %v. Error: %+v", source, target, err)
		metric.FailureCounter.WithLabelValues(compressAOFMetricLabel, "").Inc()
		return err
	}
	return nil
}

func (server *Server) uploadAOF() error {
	metric.RequestCounter.WithLabelValues(uploadAOFMetricLabel).Inc()
	uploader := s3manager.NewUploader(server.awsSession)
	f, err := os.Open(core.AppendFileName + compressedFileSuffix)
	if err != nil {
		log.Errorf("Failed to open compressed AOF for upload. File: %q, Error: %+v", core.AppendFileName+compressedFileSuffix, err)
		return err
	}

	attempt := 1
	for attempt <= server.properties.S3APIRetries {
		timer := prometheus.NewTimer(metric.RequestLatency.WithLabelValues(uploadAOFMetricLabel, ""))
		_, err = uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(server.properties.S3AOFBucket),
			Key:    aws.String(fmt.Sprintf("%v-%v%v", server.properties.AppName, server.properties.InstanceID, compressedFileType)),
			Body:   f,
		})
		timer.ObserveDuration()
		if err != nil {
			metric.FailureCounter.WithLabelValues(uploadAOFMetricLabel, "").Inc()
			log.Errorf("Failed to upload compressed AOF to S3. Attempt : %v. Error: %+v", attempt, err)
			attempt += 1
		} else {
			break
		}
	}
	return err
}

func (server *Server) setupAOF() error {
	metric.RequestCounter.WithLabelValues(setupAOFMetricLabel).Inc()
	timer := prometheus.NewTimer(metric.JobLatency.WithLabelValues(setupAOFMetricLabel, ""))
	defer timer.ObserveDuration()
	s3Key, err := server.getLatestAOF()
	if err != nil {
		metric.FailureCounter.WithLabelValues(setupAOFMetricLabel, "").Inc()
		return err
	}
	if s3Key == "" {
		return nil
	}
	err = server.downloadAOF(s3Key)
	if err != nil {
		metric.FailureCounter.WithLabelValues(setupAOFMetricLabel, "").Inc()
		return err
	}
	err = server.extractAOF()
	if err != nil {
		metric.FailureCounter.WithLabelValues(setupAOFMetricLabel, "").Inc()
		return err
	}
	return nil
}

func (server *Server) getLatestAOF() (string, error) {
	metric.RequestCounter.WithLabelValues(getAOFListMetricLabel).Inc()
	svc := s3.New(server.awsSession)
	input := &s3.ListObjectsInput{
		Bucket: aws.String(server.properties.S3AOFBucket),
	}

	location, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		log.Errorf("Could not load location while fetching list of AOF from S3. Error: %+v", err)
		return "", err
	}
	attempt := 1
	for attempt <= server.properties.S3APIRetries {
		timer := prometheus.NewTimer(metric.RequestLatency.WithLabelValues(getAOFListMetricLabel, ""))
		result, err := svc.ListObjects(input)
		timer.ObserveDuration()
		if err != nil {
			metric.FailureCounter.WithLabelValues(getAOFListMetricLabel, "").Inc()
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case s3.ErrCodeNoSuchBucket:
					log.Errorf("No bucket with name %v found while listing AOF.", server.properties.S3AOFBucket)
					return "", err
				default:
					log.Errorf("Error while fetching list of AOF from S3. Error: %+v", aerr.Error())
				}
			} else {
				log.Errorf("Error while fetching list of AOF from S3. Error: %+v", aerr.Error())
			}
			attempt += 1
		} else {
			if len(result.Contents) == 0 {
				log.Info("No AOF found in S3")
				return "", nil
			}

			var mostRecentKey string
			var mostRecentTimestamp = time.Date(1900, 1, 1, 1, 0, 0, 0, location) // Some date in the past

			for _, content := range result.Contents {
				lastMod := content.LastModified
				lastModLocal := lastMod.In(location)
				if mostRecentTimestamp.Before(lastModLocal) {
					mostRecentTimestamp = lastModLocal
					mostRecentKey = *content.Key
				}
			}
			return mostRecentKey, nil
		}
	}
	return "", err
}

func (server *Server) downloadAOF(s3Key string) error {
	metric.RequestCounter.WithLabelValues(downloadAOFMetricLabel).Inc()
	downloader := s3manager.NewDownloader(server.awsSession)
	f, err := os.Create(core.AppendFileName)
	if err != nil {
		log.Errorf("Failed to create a new AOF to download content into. Filename: %q, Error: %+v", core.AppendFileName, err)
		return err
	}
	attempt := 1
	for attempt < server.properties.S3APIRetries {
		timer := prometheus.NewTimer(metric.JobLatency.WithLabelValues(downloadAOFMetricLabel, ""))
		_, err = downloader.Download(f, &s3.GetObjectInput{
			Bucket: aws.String(server.properties.S3AOFBucket),
			Key:    aws.String(s3Key),
		})
		timer.ObserveDuration()
		if err != nil {
			metric.FailureCounter.WithLabelValues(downloadAOFMetricLabel, "").Inc()
			log.Errorf("Failed to download AOF from S3. Err: %+v", err)
			attempt += 1
		} else {
			break
		}
	}
	return err
}

func (server *Server) extractAOF() error {
	metric.RequestCounter.WithLabelValues(extractAOFMetricLabel).Inc()
	timer := prometheus.NewTimer(metric.JobLatency.WithLabelValues(extractAOFMetricLabel, ""))
	defer timer.ObserveDuration()
	source := core.AppendFileName + compressedFileSuffix
	target := core.AppendFileName
	reader, err := os.Open(source)
	if err != nil {
		metric.FailureCounter.WithLabelValues(extractAOFMetricLabel, "").Inc()
		log.Errorf("Error while opening compressed AOF to decompress. Source Location: %v. Error: %+v", source, err)
		return err
	}
	defer reader.Close()

	archive, err := gzip.NewReader(reader)
	if err != nil {
		metric.FailureCounter.WithLabelValues(extractAOFMetricLabel, "").Inc()
		log.Errorf("Error while reading compressed AOF to copy. Source Location: %v. Error: %+v", source, err)
		return err
	}
	defer archive.Close()

	writer, err := os.Create(target)
	if err != nil {
		metric.FailureCounter.WithLabelValues(extractAOFMetricLabel, "").Inc()
		log.Errorf("Error while creating empty AOF to copy data into. Target Location: %v. Error: %+v", target, err)
		return err
	}
	defer writer.Close()

	_, err = io.Copy(writer, archive)
	if err != nil {
		metric.FailureCounter.WithLabelValues(extractAOFMetricLabel, "").Inc()
		log.Errorf("Error while copying compressed AOF to AOF. Source Location: %v. Target Location: %v Error: %+v", source, target, err)
		return err
	}
	return nil
}
