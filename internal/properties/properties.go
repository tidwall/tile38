package properties

import (
	"os"
	"strconv"
)

const (
	AppName              = "APP_NAME"               //Rock App Name
	InstanceID           = "INSTANCE_ID"            //Rock instance ID
	S3AOFBucket          = "S3_AOF_BUCKET"          //S3 Bucket where AOF is uploaded
	AWSAccessKeyId       = "AWS_ACCESS_KEY_ID"      //AWS Access Key
	AWSSecretAccessKey   = "AWS_SECRET_ACCESS_KEY"  //AWS Secret Key
	AWSRegion            = "AWS_REGION"             //AWS Regions where AOF S3 bucket is
	AOFUploadCronSpec    = "AOF_UPLOAD_CRON_SPEC"   //Spec for cron that will upload AOF to S3
	S3APIRetries         = "S3_API_RETRIES"         //Retry count for S3 APIs like Upload, Download, List objects, etc.
	S3APIRetryDelayInSec = "S3_API_RETRY_DELAY_SEC" //Time between retries while hitting S3 for various operations
	PushGatewayAddr      = "PUSH_GATEWAY_ADDRESS"   //Address of the push gateway for metrics
	PushIntervalInSec    = "PUSH_INTERVAL_SEC"      //Frequency of pushing metrics to the push gateway
)

const (
	defaultAppName              = ""
	defaultInstanceID           = ""
	defaultS3AOFBucket          = ""
	defaultAWSAccessKeyId       = ""
	defaultAWSSecretAccessKey   = ""
	defaultAWSRegion            = ""
	defaultAOFUploadCronSpec    = "30 * * * *"
	defaultS3APIRetries         = 3
	defaultS3APIRetryDelayInSec = 10
	defaultPushGatewayAddr      = "localhost:9121"
	defaultPushIntervalInSec    = 30
)

type Properties struct {
	AppName              string
	InstanceID           string
	S3AOFBucket          string
	AWSAccessKeyId       string
	AWSSecretAccessKey   string
	AWSRegion            string
	AOFUploadCronSpec    string
	S3APIRetries         int
	S3APIRetryDelayInSec int
	PushGatewayAddr      string
	PushIntervalInSec    int
}

// Initializes a new Properties struct
func Initialize() *Properties {
	return &Properties{
		AppName:              getEnv(AppName, defaultAppName),
		InstanceID:           getEnv(InstanceID, defaultInstanceID),
		S3AOFBucket:          getEnv(S3AOFBucket, defaultS3AOFBucket),
		AWSAccessKeyId:       getEnv(AWSAccessKeyId, defaultAWSAccessKeyId),
		AWSSecretAccessKey:   getEnv(AWSSecretAccessKey, defaultAWSSecretAccessKey),
		AWSRegion:            getEnv(AWSRegion, defaultAWSRegion),
		AOFUploadCronSpec:    getEnv(AOFUploadCronSpec, defaultAOFUploadCronSpec),
		S3APIRetries:         getEnvAsInt(S3APIRetries, defaultS3APIRetries),
		S3APIRetryDelayInSec: getEnvAsInt(S3APIRetryDelayInSec, defaultS3APIRetryDelayInSec),
		PushGatewayAddr:      getEnv(PushGatewayAddr, defaultPushGatewayAddr),
		PushIntervalInSec:    getEnvAsInt(PushIntervalInSec, defaultPushIntervalInSec),
	}
}

// Simple helper function to read an environment or return a default value
func getEnv(key string, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}

// Simple helper function to read an environment variable into integer or return a default value
func getEnvAsInt(name string, defaultVal int) int {
	valueStr := getEnv(name, "")
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}

	return defaultVal
}

// Helper to read an environment variable into a bool or return default value
func getEnvAsBool(name string, defaultVal bool) bool {
	valStr := getEnv(name, "")
	if val, err := strconv.ParseBool(valStr); err == nil {
		return val
	}

	return defaultVal
}
