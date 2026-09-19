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

package coordinator

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Verifier proves that a backup can really be restored, which no checksum can:
// it is what finds an archive that is intact but that ZincSearch cannot use.
type Verifier interface {
	// Deep restores the archive somewhere disposable and checks what came out
	// against what the backup claims. A nil error means the backup is good.
	Deep(ctx context.Context, archivePath string) error
}

// ExecVerifier runs the ZincSearch binary: `zincsearch verify-backup --deep`,
// in a throwaway data directory. The binary must be the version of the nodes
// that made the backup, or a newer one.
type ExecVerifier struct {
	// Binary is the path of the zincsearch executable.
	Binary string
	// WorkDir is where the throwaway data directory is made. Default: os.TempDir().
	WorkDir string
}

// Deep implements Verifier.
func (v ExecVerifier) Deep(ctx context.Context, archivePath string) error {
	dir, err := os.MkdirTemp(v.WorkDir, "zinc-verify-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	abs, err := filepath.Abs(archivePath)
	if err != nil {
		return err
	}
	secret := make([]byte, 12)
	_, _ = rand.Read(secret)
	cmd := exec.CommandContext(ctx, v.Binary, "verify-backup", "--deep", abs)
	cmd.Dir = dir
	// A clean environment: nothing of the coordinator's settings may reach the
	// scratch node, above all not a stream to consume.
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"ZINC_DATA_PATH=" + filepath.Join(dir, "data"),
		"ZINC_FIRST_ADMIN_USER=verify",
		"ZINC_FIRST_ADMIN_PASSWORD=" + hex.EncodeToString(secret),
		"ZINC_STREAM_ENABLE=false",
		"ZINC_LOG_LEVEL=error",
		"ZINC_TELEMETRY=false",
		"ZINC_SENTRY=false",
	}
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(out.String())
		if len(msg) > 2000 {
			msg = msg[len(msg)-2000:]
		}
		return fmt.Errorf("%w: %s", err, msg)
	}
	return nil
}
