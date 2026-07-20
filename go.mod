module codeberg.org/Sylos/Migration-Engine

go 1.26.0

require (
	codeberg.org/Sylos/Spectra v0.2.6
	codeberg.org/Sylos/Sylos-FS v0.1.6
	codeberg.org/Sylos/go-path-linter v0.0.0
	github.com/google/uuid v1.6.0
	github.com/marcboeker/go-duckdb v1.7.0
)

require (
	github.com/apache/arrow/go/v14 v14.0.2 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	gonum.org/v1/gonum v0.16.0 // indirect
)

require (
	github.com/kr/fs v0.1.0 // indirect
	github.com/pkg/sftp v1.13.10 // indirect
	golang.org/x/crypto v0.53.0 // indirect
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
	codeberg.org/Sylos/go-path-linter => ../go-path-linter
)
