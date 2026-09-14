// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"

	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
)

// FS credential roles (match roots.SetRootRequest.Role).
const (
	FSCredentialRoleSource      = "source"
	FSCredentialRoleDestination = "destination"
)

// FSCredentialBinding is one row from fs_credential_binding.
type FSCredentialBinding struct {
	Role             string
	ConnectionID     string
	CredsConfRelPath string
	ServiceID        string
	RootFolderJSON   string
}

func (s *migrationStore) UpsertFSCredentialBinding(binding FSCredentialBinding) error {
	ops := s.ops()
	if ops == nil {
		return fmt.Errorf("UpsertFSCredentialBinding requires store db")
	}
	if binding.Role != FSCredentialRoleSource && binding.Role != FSCredentialRoleDestination {
		return fmt.Errorf("invalid credential role %q", binding.Role)
	}
	return ops.PutFSBinding(opsdb.FSBindingRecord{
		Side:           binding.Role,
		ConnectionID:   binding.ConnectionID,
		CredsPath:      binding.CredsConfRelPath,
		ServiceID:      binding.ServiceID,
		RootFolderJSON: binding.RootFolderJSON,
	})
}

func (s *migrationStore) getFSCredentialBinding(role string) (*FSCredentialBinding, error) {
	ops := s.ops()
	if ops == nil {
		return nil, fmt.Errorf("getFSCredentialBinding requires store db")
	}
	rec, ok, err := ops.GetFSBinding(role)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("fs binding %s not found", role)
	}
	return &FSCredentialBinding{
		Role:             rec.Side,
		ConnectionID:     rec.ConnectionID,
		CredsConfRelPath: rec.CredsPath,
		ServiceID:        rec.ServiceID,
		RootFolderJSON:   rec.RootFolderJSON,
	}, nil
}

func (s *migrationStore) listFSCredentialBindings() ([]FSCredentialBinding, error) {
	ops := s.ops()
	if ops == nil {
		return nil, fmt.Errorf("listFSCredentialBindings requires store db")
	}
	recs, err := ops.ListFSBindings()
	if err != nil {
		return nil, err
	}
	out := make([]FSCredentialBinding, 0, len(recs))
	seen := map[string]struct{}{}
	for _, rec := range recs {
		if _, ok := seen[rec.Side]; ok {
			continue
		}
		seen[rec.Side] = struct{}{}
		out = append(out, FSCredentialBinding{
			Role:             rec.Side,
			ConnectionID:     rec.ConnectionID,
			CredsConfRelPath: rec.CredsPath,
			ServiceID:        rec.ServiceID,
			RootFolderJSON:   rec.RootFolderJSON,
		})
	}
	return out, nil
}
