package metric

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/tidwall/tile38/internal/properties"
	"time"

	"github.com/tidwall/tile38/internal/log"
)

var (
	FailureCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "",
		Subsystem: "",
		Name:      "failure_counter",
		Help:      "Count of failures",
	}, []string{"api", "code"})

	RequestCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "",
		Subsystem: "",
		Name:      "request_counter",
		Help:      "Request Counter",
	}, []string{"api"})


	RequestLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "",
		Subsystem: "",
		Name:      "request_latency",
		Help:      "Request Latency",
		Buckets:   prometheus.LinearBuckets(0.01, 0.01, 20),
	}, []string{"api", "code"})

	JobLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "",
		Subsystem: "",
		Name:      "job_latency",
		Help:      "Latency Metric for long running jobs",
		Buckets:   prometheus.LinearBuckets(1, 1, 10),
	}, []string{"job", "response"})
)

func init() {
	registry := prometheus.NewRegistry()
	registry.MustRegister(FailureCounter)
	registry.MustRegister(RequestCounter)
	registry.MustRegister(RequestLatency)
	registry.MustRegister(JobLatency)

	prop := properties.Initialize()

	go func(registry *prometheus.Registry) {
		for {
			err := push.New(prop.PushGatewayAddr, prop.AppName).Gatherer(registry).Push()
			if err != nil {
				log.Infof("Error pushing metrics to push gateway. Error: %+v", err)
			}
			time.Sleep(time.Duration(prop.PushIntervalInSec) * time.Second)
		}
	}(registry)
}
