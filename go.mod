module codeberg.org/Sylos/Migration-Engine

go 1.26.0

require (
	codeberg.org/Sylos/Spectra v0.2.6
	codeberg.org/Sylos/Sylos-FS v0.1.6
	codeberg.org/Sylos/go-path-linter v0.0.0
	github.com/dgraph-io/badger/v4 v4.8.0
	github.com/google/uuid v1.6.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dgraph-io/ristretto/v2 v2.2.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/kr/fs v0.1.0 // indirect
	github.com/pkg/sftp v1.13.10 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.43.0 // indirect
	go.opentelemetry.io/otel/metric v1.43.0 // indirect
	go.opentelemetry.io/otel/trace v1.43.0 // indirect
	golang.org/x/crypto v0.53.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

require (
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/klauspost/compress v1.18.3 // indirect
	go.etcd.io/bbolt v1.4.3 // indirect; indirect (required by Spectra)
	golang.org/x/sys v0.46.0 // indirect
)

replace (
	codeberg.org/Sylos/Spectra => ../Spectra
	codeberg.org/Sylos/Sylos-FS => ../Sylos-FS
	codeberg.org/Sylos/go-path-linter => ../go-path-linter
)
