module github.com/Project-Sylos/Migration-Engine

go 1.24.2

require (
	github.com/Project-Sylos/Spectra v0.2.56
	github.com/Project-Sylos/Sylos-FS v0.1.2
	github.com/google/uuid v1.6.0
	github.com/oklog/ulid/v2 v2.1.1
	go.etcd.io/bbolt v1.4.3
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/kr/pretty v0.3.1 // indirect
	github.com/rogpeppe/go-internal v1.13.1 // indirect
	golang.org/x/sys v0.34.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
)

replace github.com/Project-Sylos/Spectra => ../Spectra
