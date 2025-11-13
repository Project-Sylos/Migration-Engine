// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"fmt"
	"os"

	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
)

func main() {
	addr := "127.0.0.1:8081"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}

	fmt.Printf("=== Log Listener ===\n")

	if err := logservice.RunListener(addr); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}
