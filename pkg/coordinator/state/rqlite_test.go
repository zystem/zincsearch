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

package state_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zincsearch/zincsearch/pkg/coordinator/state"
	"github.com/zincsearch/zincsearch/pkg/coordinator/state/statetest"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().String()
}

// startRQLite runs a real single node rqlite server. The test is skipped when no
// rqlited binary is around: put it in PATH or point RQLITED at it.
func startRQLite(t *testing.T) string {
	t.Helper()
	bin := os.Getenv("RQLITED")
	if bin == "" {
		bin, _ = exec.LookPath("rqlited")
	}
	if bin == "" {
		t.Skip("rqlited not found (set RQLITED or put it in PATH)")
	}
	httpAddr, raftAddr := freeAddr(t), freeAddr(t)
	cmd := exec.Command(bin, "-node-id", "1", "-http-addr", httpAddr, "-raft-addr", raftAddr, t.TempDir())
	out, err := os.CreateTemp(t.TempDir(), "rqlited-*.log")
	require.NoError(t, err)
	cmd.Stdout, cmd.Stderr = out, out
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := http.Get("http://" + httpAddr + "/readyz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return "http://" + httpAddr
			}
		}
		if time.Now().After(deadline) {
			log, _ := os.ReadFile(out.Name())
			t.Fatalf("rqlited did not become ready: %v\n%s", err, log)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestRQLite(t *testing.T) {
	url := startRQLite(t)
	statetest.Run(t, func(t *testing.T) state.Store {
		s, err := state.OpenRQLite(context.Background(), url)
		require.NoError(t, err)
		return s
	})
}

func TestRQLiteUnreachable(t *testing.T) {
	_, err := state.OpenRQLite(context.Background(), fmt.Sprintf("http://%s", freeAddr(t)))
	require.Error(t, err)
}
