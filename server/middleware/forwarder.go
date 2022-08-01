package middleware

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/types"
	"github.com/tigrisdata/tigris/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

var (
	UserAgent = "tigris-forwarder/" + util.Version
)

type Forwarder struct {
	clients sync.Map
}

var forwarder = Forwarder{}

func requestToResponse(method string) (proto.Message, proto.Message) {
	switch strings.TrimPrefix(method, "/tigrisdata.v1.Tigris/") {
	case "Insert":
		return &api.InsertRequest{}, &api.InsertResponse{}
	case "Replace":
		return &api.ReplaceRequest{}, &api.ReplaceResponse{}
	case "Update":
		return &api.UpdateRequest{}, &api.UpdateResponse{}
	case "Delete":
		return &api.DeleteRequest{}, &api.DeleteResponse{}
	case "Read":
		return &api.ReadRequest{}, &api.ReadResponse{}
	case "Events":
		return &api.EventsRequest{}, &api.EventsResponse{}
	case "Search":
		return &api.SearchRequest{}, &api.SearchResponse{}
	case "CreateOrUpdateCollection":
		return &api.CreateOrUpdateCollectionRequest{}, &api.CreateOrUpdateCollectionResponse{}
	case "DropCollection":
		return &api.DropCollectionRequest{}, &api.DropCollectionResponse{}
	case "CommitTransaction":
		return &api.CommitTransactionRequest{}, &api.CommitTransactionResponse{}
	case "RollbackTransaction":
		return &api.RollbackTransactionRequest{}, &api.RollbackTransactionResponse{}
	}
	return nil, nil
}

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

	conn, err := grpc.DialContext(ctx, fmt.Sprintf("%s:%d", origin, config.DefaultConfig.Server.Port), opts...)
	if err != nil {
		return nil, err
	}

	forwarder.clients.Store(origin, conn)

	return conn, nil
}

func proxyDirector(ctx context.Context, method string) (context.Context, *grpc.ClientConn, proto.Message, proto.Message, error) {
	client, err := getClient(ctx, api.GetTransaction(ctx).GetOrigin())

	req, resp := requestToResponse(method)
	md, _ := metadata.FromIncomingContext(ctx)

	return metadata.NewOutgoingContext(ctx, md), client, req, resp, err
}

func forwarderStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		txCtx := api.GetTransaction(stream.Context())

		if txCtx != nil && txCtx.GetOrigin() != types.MyOrigin {
			err := proxyHandler(srv, stream)
			log.Err(err).Str("method", info.FullMethod).Msg("forwarded stream request")
			return err
		}

		return handler(srv, stream)
	}
}

func forwardRequest(ctx context.Context, method string, req proto.Message) (interface{}, error) {
	oCtx, client, _, resp, err := proxyDirector(ctx, method)
	if err != nil {
		return nil, err
	}

	if err := client.Invoke(oCtx, method, req, resp); err != nil {
		return nil, err
	}

	return resp, nil
}

func forwarderUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (iface interface{}, err error) {
		txCtx := api.GetTransaction(ctx)

		if txCtx != nil && txCtx.GetOrigin() != types.MyOrigin {
			iface, err := forwardRequest(ctx, info.FullMethod, req.(proto.Message))
			log.Err(err).Str("method", info.FullMethod).Msg("forwarded request")
			return iface, err
		}

		return handler(ctx, req)
	}
}

func proxyHandler(_ interface{}, serverStream grpc.ServerStream) error {
	fullMethodName, ok := grpc.MethodFromServerStream(serverStream)
	if !ok {
		return api.Errorf(api.Code_INTERNAL, "failed to determine method name")
	}

	outgoingCtx, backendConn, req, resp, err := proxyDirector(serverStream.Context(), fullMethodName)
	if err != nil {
		return err
	}

	clientCtx, clientCancel := context.WithCancel(outgoingCtx)
	defer clientCancel()

	clientStream, err := grpc.NewClientStream(clientCtx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, backendConn, fullMethodName)
	if err != nil {
		return err
	}

	ret := make(chan error, 1)

	// start up and down streams
	forwardDownstream(serverStream, req, clientStream, ret)
	forwardUpstream(clientStream, resp, serverStream, ret)

	err = <-ret // wait for one to finish

	if err != io.EOF && err != nil {
		clientCancel() // cancel the other one in the case of error
		return err
	}

	err = <-ret // wait for the other to finish

	if err != io.EOF && err != nil {
		return err
	}

	return nil
}

func forwardUpstream(src grpc.ClientStream, resp proto.Message, dst grpc.ServerStream, ret chan error) {
	go func() {
		var err error
		defer func() {
			dst.SetTrailer(src.Trailer())
			ret <- err
		}()
		if err = src.RecvMsg(resp); err != nil {
			return
		}
		// send header back upstream after first message
		var md metadata.MD
		if md, err = src.Header(); err != nil {
			return
		}
		if err = dst.SendHeader(md); err != nil {
			return
		}
		if err = dst.SendMsg(resp); err != nil {
			return
		}
		for {
			if err = src.RecvMsg(resp); err != nil {
				break
			}
			if err = dst.SendMsg(resp); err != nil {
				break
			}
		}
	}()
}

func forwardDownstream(src grpc.ServerStream, req proto.Message, dst grpc.ClientStream, ret chan error) {
	go func() {
		for {
			if err := src.RecvMsg(req); err != nil {
				ret <- err
				break
			}
			if err := dst.SendMsg(req); err != nil {
				ret <- err
				break
			}
		}
		_ = dst.CloseSend()
	}()
}
