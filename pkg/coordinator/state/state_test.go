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
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"github.com/zincsearch/zincsearch/pkg/coordinator/state"
	"github.com/zincsearch/zincsearch/pkg/coordinator/state/statetest"
)

func TestMem(t *testing.T) {
	statetest.Run(t, func(t *testing.T) state.Store { return state.NewMem() })
}

func startNATS(t *testing.T) jetstream.JetStream {
	t.Helper()
	ns, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	require.NoError(t, err)
	go ns.Start()
	require.True(t, ns.ReadyForConnections(15*time.Second))
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	return js
}

func TestNATSKV(t *testing.T) {
	js := startNATS(t)
	statetest.Run(t, func(t *testing.T) state.Store {
		s, err := state.OpenNATSKV(context.Background(), js, "coordinator_test", 1)
		require.NoError(t, err)
		return s
	})
}
