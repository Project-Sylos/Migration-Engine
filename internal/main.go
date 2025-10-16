// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"github.com/Project-Sylos/Migration-Engine/internal/configs"
	"github.com/Project-Sylos/Migration-Engine/internal/db"
	"github.com/Project-Sylos/Migration-Engine/internal/logservice"
	"time"
)

func main() {
	buffersCfg, _ := configs.LoadBufferConfig("internal/configs")
	myDB, _ := db.NewDB("sylos.duckdb")

	// Register every known table from tables.go
	tables := []db.TableDef{
		db.SrcNodesTable{},
		db.DstNodesTable{},
		db.LogsTable{},
	}

	for _, tbl := range tables {
		name := tbl.Name()
		if cfg, ok := buffersCfg[name]; ok {
			myDB.RegisterTable(tbl, true, cfg.BatchSize, time.Duration(cfg.FlushIntervalSec)*time.Second)
		} else {
			myDB.RegisterTable(tbl, false, 0, 0)
		}
	}

	logservice.StartListener("127.0.0.1:1997")

}
