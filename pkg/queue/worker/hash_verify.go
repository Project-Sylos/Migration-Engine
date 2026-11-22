// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"codeberg.org/Sylos/Migration-Engine/pkg/logservice"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue"
	"codeberg.org/Sylos/Migration-Engine/pkg/queue/observe"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

const hashVerifyBufSize = 64 * 1024

// streamFileSHA256 opens path via OpenRead and hashes bytes into discard.
func streamFileSHA256(
	ctx context.Context,
	adapter types.FSAdapter,
	serviceID string,
	wd *observe.ProgressWatchdog,
	q *queue.Queue,
) (string, error) {
	if adapter == nil || serviceID == "" {
		return "", fmt.Errorf("missing adapter or service id")
	}
	r, err := adapter.OpenRead(ctx, serviceID)
	if err != nil {
		return "", err
	}
	defer r.Close()
	h := sha256.New()
	buf := make([]byte, hashVerifyBufSize)
	for {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		n, readErr := r.Read(buf)
		if n > 0 {
			if _, werr := h.Write(buf[:n]); werr != nil {
				return "", werr
			}
			if wd != nil {
				wd.Beat()
			}
			if q != nil && q.HasWatchdog() {
				q.BeatWatchdog()
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return "", readErr
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func hashVerifyPair(
	ctx context.Context,
	srcAdapter, dstAdapter types.FSAdapter,
	src, dst types.File,
	displayPath string,
	srcNodeID string,
	wd *observe.ProgressWatchdog,
	q *queue.Queue,
) (equal bool, srcHash, dstHash string, err error) {
	srcHash, err = streamFileSHA256(ctx, srcAdapter, src.ServiceID, wd, q)
	if err != nil {
		return false, "", "", fmt.Errorf("hash source: %w", err)
	}
	dstHash, err = streamFileSHA256(ctx, dstAdapter, dst.ServiceID, wd, q)
	if err != nil {
		return false, srcHash, "", fmt.Errorf("hash destination: %w", err)
	}
	equal = srcHash == dstHash
	if logservice.LS != nil {
		msg := fmt.Sprintf(
			"hash verify: path=%s equal=%v src_sha256=%s dst_sha256=%s",
			displayPath, equal, srcHash, dstHash,
		)
		_ = logservice.LS.LogStructured("trace", msg, "worker", "",
			logservice.LogFields{Action: "hash_verify", NodeID: srcNodeID, Queue: "dst"})
	}
	return equal, srcHash, dstHash, nil
}
