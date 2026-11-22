# Configs

This directory holds `spectra.json` only. It is a path asset (the default Spectra config used by the `main.go` CLI and referenced by Sylos-API / Spectra setup), not a Go package. There are no loaders here; consumers read the file themselves.

```
pkg/configs/
└── spectra.json   # Spectra simulator config (seed, API, and optional "spectra" block)
```
