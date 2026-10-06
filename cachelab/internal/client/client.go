// Package client 是面向缓存使用方的最小客户端:
// 先向 Router 询问路由,再直连节点读写;写路径自动处理迁移窗口的双写。
package client

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cachelabv1 "cachelab/gen/cachelab/v1"
)

type Client struct {
	Router cachelabv1.RouterClient

	connsMu chan struct{}
	nodes   map[string]cachelabv1.KVNodeClient
}

func Dial(routerAddr string) (*Client, error) {
	conn, err := grpc.NewClient(routerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{
		Router:  cachelabv1.NewRouterClient(conn),
		nodes:   map[string]cachelabv1.KVNodeClient{},
		connsMu: make(chan struct{}, 1),
	}, nil
}

func (c *Client) node(addr string) (cachelabv1.KVNodeClient, error) {
	c.connsMu <- struct{}{}
	defer func() { <-c.connsMu }()
	if n, ok := c.nodes[addr]; ok {
		return n, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	n := cachelabv1.NewKVNodeClient(conn)
	c.nodes[addr] = n
	return n, nil
}

func ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}

// Put 写键:按 RouteWrite 返回的主副本 + 镜像目标逐个写入。
func (c *Client) Put(key string, value []byte) error {
	cctx, cancel := ctx()
	defer cancel()
	rw, err := c.Router.RouteWrite(cctx, &cachelabv1.RouteWriteRequest{Key: key})
	if err != nil {
		return fmt.Errorf("route write: %w", err)
	}
	targets := append([]*cachelabv1.WriteTarget{rw.Primary}, rw.Mirrors...)
	for _, t := range targets {
		n, err := c.node(t.Addr)
		if err != nil {
			return err
		}
		pctx, pcancel := ctx()
		_, err = n.Put(pctx, &cachelabv1.PutRequest{Key: key, Value: value, RingVersion: t.RingVersion})
		pcancel()
		if err != nil {
			return fmt.Errorf("put to %s: %w", t.NodeId, err)
		}
	}
	return nil
}

// Get 读键。ringVersion=0 走活跃环;传旧版本可模拟在途请求。
func (c *Client) Get(key string, ringVersion int64) ([]byte, bool, error) {
	cctx, cancel := ctx()
	defer cancel()
	r, err := c.Router.Route(cctx, &cachelabv1.RouteRequest{Key: key, RingVersion: ringVersion})
	if err != nil {
		return nil, false, fmt.Errorf("route: %w", err)
	}
	n, err := c.node(r.Addr)
	if err != nil {
		return nil, false, err
	}
	gctx, gcancel := ctx()
	defer gcancel()
	resp, err := n.Get(gctx, &cachelabv1.GetRequest{Key: key, RingVersion: r.RingVersion})
	if err != nil {
		return nil, false, err
	}
	return resp.Value, resp.Found, nil
}

// Delete 删键:主副本与镜像目标都删。
func (c *Client) Delete(key string) error {
	cctx, cancel := ctx()
	defer cancel()
	rw, err := c.Router.RouteWrite(cctx, &cachelabv1.RouteWriteRequest{Key: key})
	if err != nil {
		return fmt.Errorf("route write: %w", err)
	}
	targets := append([]*cachelabv1.WriteTarget{rw.Primary}, rw.Mirrors...)
	for _, t := range targets {
		n, err := c.node(t.Addr)
		if err != nil {
			return err
		}
		dctx, dcancel := ctx()
		_, err = n.Delete(dctx, &cachelabv1.DeleteRequest{Key: key, RingVersion: t.RingVersion})
		dcancel()
		if err != nil {
			return fmt.Errorf("delete on %s: %w", t.NodeId, err)
		}
	}
	return nil
}
