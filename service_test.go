// Copyright © 2024 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scratch

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestService(t *testing.T) {
	ctx := context.Background()

	mss := &multiSchemaService{
		schemaServices: make(map[string]*schemaService),
	}
	SchemaService = mss

	connectorCount := 2

	connectors := make([]*Connector, connectorCount)
	for i := 0; i < connectorCount; i++ {
		conn := &Connector{
			Name:      fmt.Sprintf("test-connector-%d", i+1),
			SchemaIDs: []string{"s1", "s2"},
		}
		connectors[i] = conn
		mss.schemaServices[fmt.Sprintf("test-connector-%d", i+1)] = &schemaService{
			schemas: make(map[string]string),
		}
	}

	var wg sync.WaitGroup
	for _, conn := range connectors {
		wg.Add(1)
		ctx := withConnectorID(ctx, conn.Name)
		go func(c *Connector) {
			defer wg.Done()
			c.Open(ctx)
			time.Sleep(time.Second)
			c.Open(ctx)
			time.Sleep(time.Second)
			c.Open(ctx)
		}(conn)
	}

	wg.Wait()
}

type Connector struct {
	Name      string
	SchemaIDs []string

	isOpen bool
}

func (c *Connector) Open(ctx context.Context) {
	for _, id := range c.SchemaIDs {
		schema, err := SchemaService.Get(ctx, id)
		if err != nil {
			if c.isOpen {
				// we already opened the connector once, we expected to get an existing schema
				panic(err)
			}

			// schema not found, create it
			err = SchemaService.Register(ctx, id, "schema:"+c.Name+":"+randomWord())
			if err != nil {
				panic(err)
			}
		}
		fmt.Printf("[%s]: schema %q: %s\n", c.Name, id, schema)
	}
	c.isOpen = true // mark the connector as open
}
