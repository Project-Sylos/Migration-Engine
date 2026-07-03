// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
)

// SpectraMountProcess runs Spectra cmd/mount and tracks mount points for cleanup.
type SpectraMountProcess struct {
	cmd        *exec.Cmd
	srcMount   string
	dstMount   string
	spectraDir string
	ready      chan error
	done       chan struct{}
}

func findSpectraRoot() (string, error) {
	if root := os.Getenv("SPECTRA_ROOT"); root != "" {
		if _, err := os.Stat(filepath.Join(root, "cmd", "mount", "main.go")); err != nil {
			return "", fmt.Errorf("SPECTRA_ROOT=%q is not a Spectra repo: %w", root, err)
		}
		return root, nil
	}

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("could not resolve test package path")
	}
	meRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
	candidate := filepath.Join(meRoot, "..", "Spectra")
	if _, err := os.Stat(filepath.Join(candidate, "cmd", "mount", "main.go")); err != nil {
		return "", fmt.Errorf("Spectra repo not found at %s (set SPECTRA_ROOT): %w", candidate, err)
	}
	return candidate, nil
}

func resolveConfigPath() (string, error) {
	if cfg := os.Getenv("SPECTRA_FUSE_CONFIG"); cfg != "" {
		return filepath.Abs(cfg)
	}
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("could not resolve test package path")
	}
	cfg := filepath.Join(filepath.Dir(file), "..", "shared", "spectra_ephemeral_fuse.json")
	return filepath.Abs(cfg)
}

// StartSpectraMounts launches Spectra FUSE mounts for primary (src) and s1 (dst).
func StartSpectraMounts(srcMount, dstMount, configPath string) (*SpectraMountProcess, error) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		return nil, fmt.Errorf("FUSE mount test requires linux or darwin (got %s)", runtime.GOOS)
	}

	spectraDir, err := findSpectraRoot()
	if err != nil {
		return nil, err
	}

	for _, path := range []string{srcMount, dstMount} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			return nil, fmt.Errorf("create mount dir %s: %w", path, err)
		}
	}

	cmd := exec.Command(
		"go", "run", "./cmd/mount",
		"--mount", "primary:"+srcMount,
		"--mount", "s1:"+dstMount,
		configPath,
	)
	cmd.Dir = spectraDir
	cmd.Env = os.Environ()

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("stderr pipe: %w", err)
	}

	proc := &SpectraMountProcess{
		cmd:        cmd,
		srcMount:   srcMount,
		dstMount:   dstMount,
		spectraDir: spectraDir,
		ready:      make(chan error, 1),
		done:       make(chan struct{}),
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start spectra mount: %w", err)
	}

	go proc.consumeOutput(stdout, stderr)
	go func() {
		err := cmd.Wait()
		close(proc.done)
		if err != nil {
			select {
			case proc.ready <- fmt.Errorf("spectra mount exited: %w", err):
			default:
			}
		}
	}()

	return proc, nil
}

func (p *SpectraMountProcess) consumeOutput(stdout, stderr io.Reader) {
	var mu sync.Mutex
	lines := make([]string, 0, 8)
	primaryReady := false
	s1Ready := false

	record := func(line string) {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, line)
		fmt.Println("[spectra-mount]", line)
		if strings.Contains(line, `Mounted world "primary"`) {
			primaryReady = true
		}
		if strings.Contains(line, `Mounted world "s1"`) {
			s1Ready = true
		}
		if primaryReady && s1Ready {
			select {
			case p.ready <- nil:
			default:
			}
		}
	}

	var wg sync.WaitGroup
	for _, r := range []io.Reader{stdout, stderr} {
		wg.Add(1)
		go func(reader io.Reader) {
			defer wg.Done()
			sc := bufio.NewScanner(reader)
			for sc.Scan() {
				record(sc.Text())
			}
		}(r)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if !primaryReady || !s1Ready {
		select {
		case p.ready <- fmt.Errorf("mount process output missing ready lines (got: %v)", lines):
		default:
		}
	}
}

// WaitReady blocks until both worlds are mounted or timeout elapses.
func (p *SpectraMountProcess) WaitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case err := <-p.ready:
			if err != nil {
				return err
			}
			if err := p.pollMountPoints(5 * time.Second); err != nil {
				return err
			}
			return nil
		case <-p.done:
			return fmt.Errorf("spectra mount process exited before becoming ready")
		default:
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for Spectra FUSE mounts after %s", timeout)
}

func (p *SpectraMountProcess) pollMountPoints(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		srcOK := mountPointAccessible(p.srcMount)
		dstOK := mountPointAccessible(p.dstMount)
		if srcOK && dstOK {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("mount points not accessible: src=%s dst=%s", p.srcMount, p.dstMount)
}

func mountPointAccessible(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	if !fi.IsDir() {
		return false
	}
	_, err = os.ReadDir(path)
	return err == nil
}

// Stop sends SIGINT for graceful unmount, then force-kills if needed.
func (p *SpectraMountProcess) Stop() {
	if p.cmd.Process != nil {
		_ = p.cmd.Process.Signal(syscall.SIGINT)
	}

	select {
	case <-p.done:
	case <-time.After(15 * time.Second):
		if p.cmd.Process != nil {
			_ = p.cmd.Process.Kill()
		}
		<-p.done
	}

	for _, path := range []string{p.srcMount, p.dstMount} {
		unmountPath(path)
	}
}

func unmountPath(path string) {
	if path == "" {
		return
	}
	if _, err := exec.LookPath("fusermount"); err == nil {
		cmd := exec.Command("fusermount", "-u", path)
		_ = cmd.Run()
		return
	}
	if _, err := exec.LookPath("umount"); err == nil {
		cmd := exec.Command("umount", path)
		_ = cmd.Run()
	}
}
