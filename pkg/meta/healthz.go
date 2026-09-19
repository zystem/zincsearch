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

package meta

import (
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
)

var (
	healthLock    sync.RWMutex
	healthDetails = make(map[string]func() interface{})
)

// RegisterHealthDetail adds a named section to the /healthz response, e.g. the
// replication state of the node. The endpoint stays 200 regardless of the
// detail, so a node that lost its stream keeps serving searches.
func RegisterHealthDetail(name string, detail func() interface{}) {
	healthLock.Lock()
	healthDetails[name] = detail
	healthLock.Unlock()
}

// GetHealthz function gets all events
//
// @Id Healthz
// @Summary Get healthz
// @Produce json
// @Success 200 {object} HealthzResponse
// @Router /healthz [get]
func GetHealthz(c *gin.Context) {
	resp := HealthzResponse{Status: "ok"}
	healthLock.RLock()
	details := make(map[string]func() interface{}, len(healthDetails))
	for name, detail := range healthDetails {
		details[name] = detail
	}
	healthLock.RUnlock()
	if len(details) > 0 {
		resp.Details = make(map[string]interface{}, len(details))
		for name, detail := range details {
			resp.Details[name] = detail()
		}
	}
	c.JSON(http.StatusOK, resp)
}

type HealthzResponse struct {
	Status  string                 `json:"status"`
	Details map[string]interface{} `json:"details,omitempty"`
}
