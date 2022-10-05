module github.com/tidwall/tile38

go 1.19

require (
	github.com/Shopify/sarama v1.13.0
	github.com/aws/aws-sdk-go v1.44.112
	github.com/eclipse/paho.mqtt.golang v1.1.0
	github.com/golang/protobuf v1.5.2
	github.com/gomodule/redigo v2.0.1-0.20181026001555-e8fc0692a7e2+incompatible
	github.com/mmcloughlin/geohash v0.0.0-20181009053802-f7f2bcae3294
	github.com/nats-io/go-nats v1.6.0
	github.com/peterh/liner v1.0.1-0.20170902204657-a37ad3984311
	github.com/prometheus/client_golang v1.12.0
	github.com/streadway/amqp v0.0.0-20170926065634-cefed15a0bd8
	github.com/tidwall/btree v0.0.0-20170113224114-9876f1454cf0
	github.com/tidwall/buntdb v1.1.0
	github.com/tidwall/geoindex v1.4.0
	github.com/tidwall/geojson v1.1.13
	github.com/tidwall/gjson v1.13.0
	github.com/tidwall/match v1.1.1
	github.com/tidwall/pretty v1.2.0
	github.com/tidwall/redbench v0.0.0-20181110173744-17c5b5b864a4
	github.com/tidwall/redcon v0.0.0-20171003141744-3df12143a4fe
	github.com/tidwall/resp v0.0.0-20160908231031-b2b1a7ca20e3
	github.com/tidwall/rhh v1.1.0
	github.com/tidwall/sjson v1.1.1
	github.com/tidwall/tinybtree v0.0.0-20181217131827-de5932d649b5
	github.com/yuin/gopher-lua v0.0.0-20170915035107-eb1c7299435c
	golang.org/x/crypto v0.0.0-20221005025214-4161e89ecf1b
	golang.org/x/net v0.0.0-20221004154528-8021a29435af
	google.golang.org/grpc v1.31.0
	layeh.com/gopher-json v0.0.0-20161224164157-c128cc74278b
)

require (
	github.com/Shopify/toxiproxy v2.1.4+incompatible // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/eapache/go-resiliency v1.0.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20160609142408-bb955e01b934 // indirect
	github.com/eapache/queue v1.0.2 // indirect
	github.com/golang/snappy v0.0.0-20170215233205-553a64147049 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/nats-io/gnatsd v1.4.1 // indirect
	github.com/nats-io/nuid v1.0.0 // indirect
	github.com/pierrec/lz4 v1.0.1 // indirect
	github.com/pierrec/xxHash v0.1.1 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.32.1 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20161128210544-1f30fe9094a5 // indirect
	github.com/tidwall/cities v0.0.0-20190730194520-dbe1ae0b862c // indirect
	github.com/tidwall/grect v0.0.0-20161006141115-ba9a043346eb // indirect
	github.com/tidwall/lotsa v1.0.1 // indirect
	github.com/tidwall/rbang v1.2.2 // indirect
	github.com/tidwall/rtree v0.0.0-20180113144539-6cd427091e0e // indirect
	github.com/tidwall/tinyqueue v0.0.0-20180302190814-1e39f5511563 // indirect
	golang.org/x/sys v0.0.0-20220928140112-f11e5e49a4ec // indirect
	golang.org/x/term v0.0.0-20210927222741-03fcf44c2211 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/genproto v0.0.0-20200825200019-8632dd797987 // indirect
	google.golang.org/protobuf v1.26.0 // indirect
)

replace github.com/tidwall/geoindex => github.com/housecanary/geoindex v1.4.0-snapshot

replace github.com/tidwall/tinybtree => github.com/housecanary/tinybtree v1.0.0-snapshot

replace github.com/tidwall/btree => github.com/housecanary/btree v0.0.1-snapshot

replace github.com/tidwall/rbang => github.com/housecanary/rbang v1.1.1-snapshot

replace github.com/tidwall/geojson => github.com/housecanary/geojson v1.1.14-0.20200924195856-2632af9baba8
