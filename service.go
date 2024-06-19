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
)

type connectorIDKeyType struct{}

func withConnectorID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, connectorIDKeyType{}, id)
}
func connectorID(ctx context.Context) string {
	id := ctx.Value(connectorIDKeyType{})
	if id == nil {
		return ""
	}
	return id.(string)
}

var SchemaService interface {
	// Get returns the schema for the given id.
	Get(ctx context.Context, id string) (schema string, err error)
	// Register registers a new schema with the given id.
	Register(ctx context.Context, id, schema string) error
}

type multiSchemaService struct {
	schemaServices map[string]*schemaService
}

func (m *multiSchemaService) Get(ctx context.Context, id string) (string, error) {
	cid := connectorID(ctx)
	fmt.Printf("[multiSchemaService]: connector %s requested schema with ID %q\n", cid, id)
	s, ok := m.schemaServices[cid]
	if !ok {
		return "", fmt.Errorf("schema service for connector %s not found", cid)
	}
	schema, err := s.Get(ctx, id)
	if err != nil {
		fmt.Printf("[multiSchemaService]: connector %s failed to get schema: %s\n", cid, err)
		return "", err
	}
	fmt.Printf("[multiSchemaService]: connector %s found schema with ID %q\n", cid, id)
	return schema, nil
}

func (m *multiSchemaService) Register(ctx context.Context, id, schema string) error {
	cid := connectorID(ctx)
	fmt.Printf("[multiSchemaService]: connector %s requested to register schema %q\n", cid, schema)
	s, ok := m.schemaServices[cid]
	if !ok {
		return fmt.Errorf("schema service for connector %s not found", cid)
	}

	err := s.Register(ctx, id, schema)
	if err != nil {
		fmt.Printf("[multiSchemaService]: connector %s failed to register schema: %s\n", cid, err)
		return err
	}

	fmt.Printf("[multiSchemaService]: connector %s registered schema with ID %q\n", cid, id)
	return nil
}

type schemaService struct {
	schemas map[string]string
}

func (s *schemaService) Get(ctx context.Context, id string) (string, error) {
	schema, ok := s.schemas[id]
	if !ok {
		return "", fmt.Errorf("schema %s not found", id)
	}
	return schema, nil
}

func (s *schemaService) Register(ctx context.Context, id, schema string) error {
	if _, ok := s.schemas[id]; ok {
		return fmt.Errorf("schema %s already exists", id)
	}
	s.schemas[id] = schema
	return nil
}
