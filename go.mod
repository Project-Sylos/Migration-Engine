module codeberg.org/Sylos/Migration-Engine

go 1.25.6

require (
	codeberg.org/Sylos/Spectra v0.2.56
	codeberg.org/Sylos/Sylos-FS v0.1.2
	github.com/google/uuid v1.6.0
	github.com/marcboeker/go-duckdb v1.7.0
	go.etcd.io/bbolt v1.4.3
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/apache/arrow/go/v14 v14.0.2 // indirect
	github.com/goccy/go-json v0.10.2 // indirect
	github.com/google/flatbuffers v23.5.26+incompatible // indirect
	github.com/klauspost/compress v1.16.7 // indirect
	github.com/klauspost/cpuid/v2 v2.2.5 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.18 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	golang.org/x/mod v0.13.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/tools v0.14.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
)

replace codeberg.org/Sylos/Spectra => ../Spectra

replace codeberg.org/Sylos/Sylos-FS => ../Sylos-FS
