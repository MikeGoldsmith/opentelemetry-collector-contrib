module github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage

go 1.16

require (
	github.com/stretchr/testify v1.7.0
	go.etcd.io/bbolt v1.3.6
	go.opentelemetry.io/collector v0.29.1-0.20210702192737-aaa6d7d6b859
	go.uber.org/zap v1.18.1
)

replace go.opentelemetry.io/collector/model => go.opentelemetry.io/collector/model v0.0.0-20210702192737-aaa6d7d6b859
