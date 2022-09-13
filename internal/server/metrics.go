package server

import (
	"fmt"
	"net/http"

	"github.com/tidwall/tile38/core"
	"github.com/tidwall/tile38/internal/collection"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	metricDescriptions = map[string]*prometheus.Desc{
		/*
			these metrics are taken from basicStats() / extStats()
			by accessing the map and directly exporting the value found
		*/
		"num_collections":          prometheus.NewDesc("tile38_collections", "Total number of collections", nil, nil),
		"pid":                      prometheus.NewDesc("tile38_pid", "", nil, nil),
		"aof_size":                 prometheus.NewDesc("tile38_aof_size_bytes", "", nil, nil),
		"num_hooks":                prometheus.NewDesc("tile38_hooks", "", nil, nil),
		"in_memory_size":           prometheus.NewDesc("tile38_in_memory_size_bytes", "", nil, nil),
		"heap_size":                prometheus.NewDesc("tile38_heap_size_bytes", "", nil, nil),
		"heap_released":            prometheus.NewDesc("tile38_memory_reap_released_bytes", "", nil, nil),
		"max_heap_size":            prometheus.NewDesc("tile38_memory_max_heap_size_bytes", "", nil, nil),
		"avg_item_size":            prometheus.NewDesc("tile38_avg_item_size_bytes", "", nil, nil),
		"pointer_size":             prometheus.NewDesc("tile38_pointer_size_bytes", "", nil, nil),
		"cpus":                     prometheus.NewDesc("tile38_num_cpus", "", nil, nil),
		"tile38_connected_clients": prometheus.NewDesc("tile38_connected_clients", "", nil, nil),

		"tile38_total_connections_received": prometheus.NewDesc("tile38_connections_received_total", "", nil, nil),
		"tile38_total_messages_sent":        prometheus.NewDesc("tile38_messages_sent_total", "", nil, nil),
		"tile38_expired_keys":               prometheus.NewDesc("tile38_expired_keys_total", "", nil, nil),

		/*
			these metrics are NOT taken from basicStats() / extStats()
			but are calculated independently
		*/
		"collection_objects": prometheus.NewDesc("tile38_collection_objects", "Total number of objects per collection", []string{"col"}, nil),
		"collection_points":  prometheus.NewDesc("tile38_collection_points", "Total number of points per collection", []string{"col"}, nil),
		"collection_strings": prometheus.NewDesc("tile38_collection_strings", "Total number of strings per collection", []string{"col"}, nil),
		"collection_weight":  prometheus.NewDesc("tile38_collection_weight_bytes", "Total weight of collection in bytes", []string{"col"}, nil),
		"server_info":        prometheus.NewDesc("tile38_server_info", "Server info", []string{"id", "version"}, nil),
		"replication":        prometheus.NewDesc("tile38_replication_info", "Replication info", []string{"role", "following", "caught_up", "caught_up_once"}, nil),
		"start_time":         prometheus.NewDesc("tile38_start_time_seconds", "", nil, nil),
	}

	cmdDurations = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "tile38_cmd_duration_seconds",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001},
	}, []string{"cmd"},
	)
)

func (s *Server) MetricsIndexHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(`<html><head>
<title>Tile38 ` + core.Version + `</title></head>
<body><h1>Tile38 ` + core.Version + `</h1>
<p><a href='/metrics'>Metrics</a></p>
</body></html>`))
}

func (s *Server) MetricsHandler(w http.ResponseWriter, r *http.Request) {
	reg := prometheus.NewRegistry()

	reg.MustRegister(
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewGoCollector(),
		collectors.NewBuildInfoCollector(),
		cmdDurations,
		s,
	)

	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(w, r)
}

func (s *Server) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range metricDescriptions {
		ch <- desc
	}
}

func (s *Server) Collect(ch chan<- prometheus.Metric) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	m := make(map[string]interface{})
	s.basicStats(m)
	s.extStats(m)

	for metric, descr := range metricDescriptions {
		if val, ok := m[metric].(int); ok {
			ch <- prometheus.MustNewConstMetric(descr, prometheus.GaugeValue, float64(val))
		} else if val, ok := m[metric].(float64); ok {
			ch <- prometheus.MustNewConstMetric(descr, prometheus.GaugeValue, val)
		}
	}

	ch <- prometheus.MustNewConstMetric(
		metricDescriptions["server_info"],
		prometheus.GaugeValue, 1.0,
		s.config.serverID(), core.Version)

	ch <- prometheus.MustNewConstMetric(
		metricDescriptions["start_time"],
		prometheus.GaugeValue, float64(s.started.Unix()))

	replLbls := []string{"leader", "", "", ""}
	if s.config.followHost() != "" {
		replLbls = []string{"follower",
			fmt.Sprintf("%s:%d", s.config.followHost(), s.config.followPort()),
			fmt.Sprintf("%t", s.fcup), fmt.Sprintf("%t", s.fcuponce)}
	}
	ch <- prometheus.MustNewConstMetric(
		metricDescriptions["replication"],
		prometheus.GaugeValue, 1.0,
		replLbls...)

	/*
		add objects/points/strings stats for each collection
	*/
	s.cols.Scan(func(key string, col *collection.Collection) bool {
		ch <- prometheus.MustNewConstMetric(
			metricDescriptions["collection_objects"],
			prometheus.GaugeValue,
			float64(col.Count()),
			key,
		)
		ch <- prometheus.MustNewConstMetric(
			metricDescriptions["collection_points"],
			prometheus.GaugeValue,
			float64(col.PointCount()),
			key,
		)
		ch <- prometheus.MustNewConstMetric(
			metricDescriptions["collection_strings"],
			prometheus.GaugeValue,
			float64(col.StringCount()),
			key,
		)
		ch <- prometheus.MustNewConstMetric(
			metricDescriptions["collection_weight"],
			prometheus.GaugeValue,
			float64(col.TotalWeight()),
			key,
		)
		return true
	})
}
