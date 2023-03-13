module github.com/open-telemetry/opentelemetry-collector-contrib/internal/scrapertest

go 1.17

require (
	github.com/stretchr/testify v1.7.0
	go.opentelemetry.io/collector/model v0.45.1-0.20220222185228-27f7607ca13a
	go.uber.org/multierr v1.10.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/open-telemetry/opentelemetry-collector-contrib/receiver/scraperhelper => ../../receiver/scraperhelper
