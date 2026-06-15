// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package sparkconnect implements a minimal, focused Spark Connect gRPC client
// tailored to the needs of the ADBC driver. It speaks the Spark Connect 4.x
// protocol directly over gRPC and returns results as Apache Arrow data so that
// the ADBC layer can hand them to callers without an extra copy.
package sparkconnect

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	connect "github.com/HyukjinKwon/adbc-driver-spark/internal/sparkconnect/proto/spark/connect"
)

// Client is a connected Spark Connect session. It is safe for concurrent use
// by multiple goroutines; the underlying gRPC channel multiplexes requests.
type Client struct {
	cfg       *Config
	conn      *grpc.ClientConn
	svc       connect.SparkConnectServiceClient
	alloc     memory.Allocator
	sessionID string

	mu              sync.Mutex
	serverSessionID string
	closed          bool
}

// Dial establishes a Spark Connect session against the endpoint described by
// cfg. The supplied allocator is used for all Arrow data produced by the
// client; pass memory.DefaultAllocator if you have no specific requirement.
func Dial(ctx context.Context, cfg *Config, alloc memory.Allocator) (*Client, error) {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}

	var transportCreds credentials.TransportCredentials
	if cfg.UseTLS {
		transportCreds = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	} else {
		transportCreds = insecure.NewCredentials()
	}

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxRecvMsgSize)),
	}

	conn, err := grpc.NewClient(cfg.Endpoint(), dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("spark connect: dial %s: %w", cfg.Endpoint(), err)
	}

	sessionID := cfg.SessionID
	if sessionID == "" {
		sessionID = uuid.NewString()
	}

	c := &Client{
		cfg:       cfg,
		conn:      conn,
		svc:       connect.NewSparkConnectServiceClient(conn),
		alloc:     alloc,
		sessionID: sessionID,
	}
	return c, nil
}

// maxRecvMsgSize bounds a single gRPC message. Spark Connect streams results in
// chunks, but individual Arrow batches can be large, so we raise the default
// 4 MiB limit substantially (128 MiB).
const maxRecvMsgSize = 128 * 1024 * 1024

// SessionID returns the client-side session identifier.
func (c *Client) SessionID() string { return c.sessionID }

// Allocator returns the Arrow allocator backing this client.
func (c *Client) Allocator() memory.Allocator { return c.alloc }

// outgoingContext attaches authentication and user-supplied headers to ctx.
func (c *Client) outgoingContext(ctx context.Context) context.Context {
	md := metadata.MD{}
	if c.cfg.Token != "" {
		md.Set("authorization", "Bearer "+c.cfg.Token)
	}
	for k, v := range c.cfg.Headers {
		md.Set(k, v)
	}
	if len(md) == 0 {
		return ctx
	}
	return metadata.NewOutgoingContext(ctx, md)
}

func (c *Client) userContext() *connect.UserContext {
	if c.cfg.UserID == "" {
		return nil
	}
	return &connect.UserContext{UserId: c.cfg.UserID}
}

func (c *Client) clientType() *string {
	ua := c.cfg.UserAgent
	return &ua
}

// rememberServerSession records the server-assigned session id from a response
// so that later requests can assert continuity if desired.
func (c *Client) rememberServerSession(id string) {
	if id == "" {
		return
	}
	c.mu.Lock()
	c.serverSessionID = id
	c.mu.Unlock()
}

// ExecuteSQL runs a SQL statement and returns a RecordReader over the result.
// Positional parameters are bound as Spark Connect SQL literals when provided.
// The returned reader owns its data and must be released by the caller.
func (c *Client) ExecuteSQL(ctx context.Context, query string, posArgs []*connect.Expression_Literal) (array.RecordReader, error) {
	plan := &connect.Plan{
		OpType: &connect.Plan_Root{
			Root: &connect.Relation{
				RelType: &connect.Relation_Sql{
					Sql: &connect.SQL{
						Query:   query,
						PosArgs: posArgs,
					},
				},
			},
		},
	}
	return c.executePlan(ctx, plan)
}

func (c *Client) executePlan(ctx context.Context, plan *connect.Plan) (array.RecordReader, error) {
	opID := uuid.NewString()
	req := &connect.ExecutePlanRequest{
		SessionId:   c.sessionID,
		UserContext: c.userContext(),
		Plan:        plan,
		ClientType:  c.clientType(),
		OperationId: &opID,
	}

	stream, err := c.svc.ExecutePlan(c.outgoingContext(ctx), req)
	if err != nil {
		return nil, wrapGRPC(err)
	}
	return newResultReader(c, stream)
}

// SchemaOf returns the Arrow schema produced by a SQL statement without
// materializing any rows. It does so by executing the statement with a
// "LIMIT 0" wrapper and reading the schema embedded in the Arrow IPC stream.
func (c *Client) SchemaOf(ctx context.Context, query string) (*arrow.Schema, error) {
	wrapped := fmt.Sprintf("SELECT * FROM (%s) LIMIT 0", query)
	rdr, err := c.ExecuteSQL(ctx, wrapped, nil)
	if err != nil {
		return nil, err
	}
	defer rdr.Release()
	schema := rdr.Schema()
	// Drain so the server-side operation completes cleanly.
	for rdr.Next() {
	}
	if err := rdr.Err(); err != nil {
		return nil, err
	}
	return schema, nil
}

// Config issues a Spark Connect Config RPC to set a single runtime key.
func (c *Client) SetConf(ctx context.Context, key, value string) error {
	req := &connect.ConfigRequest{
		SessionId:   c.sessionID,
		UserContext: c.userContext(),
		ClientType:  c.clientType(),
		Operation: &connect.ConfigRequest_Operation{
			OpType: &connect.ConfigRequest_Operation_Set{
				Set: &connect.ConfigRequest_Set{
					Pairs: []*connect.KeyValue{{Key: key, Value: &value}},
				},
			},
		},
	}
	resp, err := c.svc.Config(c.outgoingContext(ctx), req)
	if err != nil {
		return wrapGRPC(err)
	}
	c.rememberServerSession(resp.GetServerSideSessionId())
	return nil
}

// GetConf issues a Spark Connect Config RPC to read a single runtime key.
func (c *Client) GetConf(ctx context.Context, key string) (string, bool, error) {
	req := &connect.ConfigRequest{
		SessionId:   c.sessionID,
		UserContext: c.userContext(),
		ClientType:  c.clientType(),
		Operation: &connect.ConfigRequest_Operation{
			OpType: &connect.ConfigRequest_Operation_Get{
				Get: &connect.ConfigRequest_Get{Keys: []string{key}},
			},
		},
	}
	resp, err := c.svc.Config(c.outgoingContext(ctx), req)
	if err != nil {
		return "", false, wrapGRPC(err)
	}
	c.rememberServerSession(resp.GetServerSideSessionId())
	for _, pair := range resp.GetPairs() {
		if pair.GetKey() == key {
			return pair.GetValue(), true, nil
		}
	}
	return "", false, nil
}

// ServerSideSessionID returns the server-assigned session id, if the server has
// reported one yet. It is empty until the first response is received.
func (c *Client) ServerSideSessionID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.serverSessionID
}

// ServerSparkVersion asks the server for its Spark version via AnalyzePlan. It
// is useful for GetInfo and for capability checks across Spark 4.0.x / 4.1.x.
func (c *Client) ServerSparkVersion(ctx context.Context) (string, error) {
	req := &connect.AnalyzePlanRequest{
		SessionId:   c.sessionID,
		UserContext: c.userContext(),
		ClientType:  c.clientType(),
		Analyze: &connect.AnalyzePlanRequest_SparkVersion_{
			SparkVersion: &connect.AnalyzePlanRequest_SparkVersion{},
		},
	}
	resp, err := c.svc.AnalyzePlan(c.outgoingContext(ctx), req)
	if err != nil {
		return "", wrapGRPC(err)
	}
	c.rememberServerSession(resp.GetServerSideSessionId())
	return resp.GetSparkVersion().GetVersion(), nil
}

// Interrupt cancels all running operations in this session. It maps onto the
// Spark Connect Interrupt RPC with INTERRUPT_TYPE_ALL.
func (c *Client) Interrupt(ctx context.Context) error {
	req := &connect.InterruptRequest{
		SessionId:     c.sessionID,
		UserContext:   c.userContext(),
		ClientType:    c.clientType(),
		InterruptType: connect.InterruptRequest_INTERRUPT_TYPE_ALL,
	}
	if _, err := c.svc.Interrupt(c.outgoingContext(ctx), req); err != nil {
		return wrapGRPC(err)
	}
	return nil
}

// Close releases the server-side session and tears down the gRPC channel.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	// Best-effort release of the remote session, with a bounded timeout so that
	// an unresponsive server can never make Close hang (it is typically called
	// from defer/shutdown paths).
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = c.svc.ReleaseSession(c.outgoingContext(ctx), &connect.ReleaseSessionRequest{
		SessionId:   c.sessionID,
		UserContext: c.userContext(),
		ClientType:  c.clientType(),
	})
	return c.conn.Close()
}
