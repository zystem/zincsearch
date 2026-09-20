/* Copyright 2022 Zinc Labs Inc. and Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

package backup

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// ErrNotFound means there is no backup of that name.
var ErrNotFound = errors.New("backup: not found")

var nameRe = regexp.MustCompile(`^zincsearch-[0-9]{8}T[0-9]{6}Z-[0-9]+(-[0-9]+)?\.tgz$`)

// ValidName reports whether name can be the name of a stored backup. Names come
// from HTTP requests, so this is what keeps them from leaving the directory.
func ValidName(name string) bool { return nameRe.MatchString(name) }

func sidecarPath(dir, name string) string { return filepath.Join(dir, name+".json") }

func writeSidecar(dir string, info *Info) error {
	raw, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(sidecarPath(dir, info.Name), raw, 0o600)
}

// List returns the backups stored in dir, oldest first.
func List(dir string) ([]Info, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var infos []Info
	for _, e := range entries {
		name := strings.TrimSuffix(e.Name(), ".json")
		if e.IsDir() || name == e.Name() || !ValidName(name) {
			continue
		}
		info, err := readInfo(dir, name)
		if err != nil {
			continue // an archive without a readable description is not listed
		}
		infos = append(infos, *info)
	}
	sort.Slice(infos, func(i, j int) bool {
		if !infos[i].CreatedAt.Equal(infos[j].CreatedAt) {
			return infos[i].CreatedAt.Before(infos[j].CreatedAt)
		}
		return infos[i].Name < infos[j].Name
	})
	return infos, nil
}

func readInfo(dir, name string) (*Info, error) {
	raw, err := os.ReadFile(sidecarPath(dir, name))
	if err != nil {
		return nil, err
	}
	info := new(Info)
	if err := json.Unmarshal(raw, info); err != nil {
		return nil, err
	}
	if info.Name != name {
		return nil, fmt.Errorf("description of %s names %s", name, info.Name)
	}
	return info, nil
}

// Open opens the archive of a stored backup for reading.
func Open(dir, name string) (*os.File, *Info, error) {
	if !ValidName(name) {
		return nil, nil, ErrNotFound
	}
	info, err := readInfo(dir, name)
	if err != nil {
		return nil, nil, ErrNotFound
	}
	f, err := os.Open(filepath.Join(dir, name))
	if err != nil {
		return nil, nil, ErrNotFound
	}
	return f, info, nil
}

// Delete removes a stored backup.
func Delete(dir, name string) error {
	if !ValidName(name) {
		return ErrNotFound
	}
	if _, err := readInfo(dir, name); err != nil {
		return ErrNotFound
	}
	// The description goes first: an archive without one is no longer listed.
	if err := os.Remove(sidecarPath(dir, name)); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}
