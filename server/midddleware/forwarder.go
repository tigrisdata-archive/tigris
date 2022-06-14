package middleware

import (
	"context"
	"sync"

	"github.com/mwitkow/grpc-proxy/proxy"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/types"
	"github.com/tigrisdata/tigris/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

var (
	UserAgent = "tigris-forwarder/" + util.Version
)

type Forwarder struct {
	clients sync.Map
}

var forwarder = Forwarder{}

// TODO: Sweep unused connections periodically
func getClient(ctx context.Context, origin string) (*grpc.ClientConn, error) {
	if c, ok := forwarder.clients.Load(origin); ok {
		return c.(*grpc.ClientConn), nil
	}

	opts := []grpc.DialOption{
		grpc.FailOnNonTempDialError(true),
		grpc.WithReturnConnectionError(),
		grpc.WithUserAgent(UserAgent),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.DialContext(ctx, origin, opts...)
	if err != nil {
		return nil, err
	}

	forwarder.clients.Store(origin, conn)

	return conn, nil
}

func proxyDirector(ctx context.Context, _ string) (context.Context, *grpc.ClientConn, error) {
	txCtx := api.GetTransaction(ctx, nil)

	client, err := getClient(ctx, txCtx.GetOrigin())

	return ctx, client, err
}

func forwarderStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		txCtx := api.GetTransaction(stream.Context(), nil)

		if txCtx != nil && txCtx.GetOrigin() != types.MyOrigin {
			return proxy.TransparentHandler(proxyDirector)(srv, stream)
		}

		return handler(srv, stream)
	}
}

func forwardRequest(ctx context.Context, txCtx *api.TransactionCtx, method string, req proto.Message) (interface{}, error) {
	client, err := getClient(ctx, txCtx.GetOrigin())
	if err != nil {
		return nil, err
	}

	resp := &anypb.Any{}
	if err := client.Invoke(ctx, method, req, resp); err != nil {
		return nil, err
	}

	return resp, nil
}

func forwarderUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (iface interface{}, err error) {
		txCtx := api.GetTransaction(ctx, req.(proto.Message))

		if txCtx != nil && txCtx.GetOrigin() != types.MyOrigin {
			return forwardRequest(ctx, txCtx, info.FullMethod, req.(proto.Message))
		}

		return handler(ctx, req)
	}
}
