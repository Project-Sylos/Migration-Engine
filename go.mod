module codeberg.org/Sylos/Migration-Engine

go 1.26.0

require (
	codeberg.org/Sylos/Spectra v0.2.6
	codeberg.org/Sylos/Sylos-FS v0.1.6
	github.com/duckdb/duckdb-go/v2 v2.10504.0
)

require github.com/google/uuid v1.6.0

require (
	github.com/apache/arrow-go/v18 v18.5.1 // indirect
	github.com/duckdb/duckdb-go-bindings v0.10504.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/darwin-amd64 v0.10504.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/darwin-arm64 v0.10504.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/linux-amd64 v0.10504.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/linux-arm64 v0.10504.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/windows-amd64 v0.10504.0 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	golang.org/x/exp v0.0.0-20260112195511-716be5621a96 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/telemetry v0.0.0-20260116145544-c6413dc483f5 // indirect
)

require (
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/klauspost/compress v1.18.3 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.25 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.etcd.io/bbolt v1.4.3 // indirect; indirect (required by Spectra)
	golang.org/x/mod v0.32.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/tools v0.41.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
)

replace (
	codeberg.org/Sylos/Spectra => ../Spectra
	codeberg.org/Sylos/Sylos-FS => ../Sylos-FS
)
