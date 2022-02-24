package api

import (
	"context"
	"net/http"

	"google.golang.org/grpc"

	"github.com/gogo/protobuf/proto"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	piface "google.golang.org/protobuf/runtime/protoiface"
)

type adaptor struct {
	proto.Message
}

func (a *adaptor) Descriptor() protoreflect.MessageDescriptor {
	//TODO implement me
	panic("implement me")
}

func (a *adaptor) Type() protoreflect.MessageType {
	//TODO implement me
	panic("implement me")
}

func (a *adaptor) New() protoreflect.Message {
	//TODO implement me
	panic("implement me")
}

func (a *adaptor) Interface() protoreflect.ProtoMessage {
	//TODO implement me
	panic("implement me")
}

func (a *adaptor) Range(f func(protoreflect.FieldDescriptor, protoreflect.Value) bool) {
	//TODO implement me
	panic("implement me")
}

func (a *adaptor) Has(descriptor protoreflect.FieldDescriptor) bool {
	//TODO implement me
	panic("implement me")
}

func (a *adaptor) Clear(descriptor protoreflect.FieldDescriptor) {
	//TODO implement me
	panic("implement me")
}

func (a *adaptor) Get(descriptor protoreflect.FieldDescriptor) protoreflect.Value {
	//TODO implement me
	panic("implement me")
}

func (a *adaptor) Set(descriptor protoreflect.FieldDescriptor, value protoreflect.Value) {
	//TODO implement me
	panic("implement me")
}

func (a *adaptor) Mutable(descriptor protoreflect.FieldDescriptor) protoreflect.Value {
	//TODO implement me
	panic("implement me")
}

func (a *adaptor) NewField(descriptor protoreflect.FieldDescriptor) protoreflect.Value {
	//TODO implement me
	panic("implement me")
}

func (a *adaptor) WhichOneof(descriptor protoreflect.OneofDescriptor) protoreflect.FieldDescriptor {
	//TODO implement me
	panic("implement me")
}

func (a *adaptor) GetUnknown() protoreflect.RawFields {
	//TODO implement me
	panic("implement me")
}

func (a *adaptor) SetUnknown(fields protoreflect.RawFields) {
	//TODO implement me
	panic("implement me")
}

func (a *adaptor) IsValid() bool {
	//TODO implement me
	panic("implement me")
}

func (a *adaptor) ProtoMethods() *piface.Methods {
	//TODO implement me
	panic("implement me")
}

func (a *adaptor) ProtoReflect() protoreflect.Message {
	return a
}

func ForwardResponseMessage(ctx context.Context, mux *runtime.ServeMux, marshaler runtime.Marshaler, w http.ResponseWriter, req *http.Request, resp proto.Message, opts ...func(context.Context, http.ResponseWriter, gproto.Message) error) {
	runtime.ForwardResponseMessage(ctx, mux, marshaler, w, req, &adaptor{resp}, opts...)
}

func ForwardResponseStream(ctx context.Context, mux *runtime.ServeMux, marshaler runtime.Marshaler, w http.ResponseWriter, req *http.Request, recv func() (proto.Message, error), opts ...func(context.Context, http.ResponseWriter, gproto.Message) error) {
	runtime.ForwardResponseStream(ctx, mux, marshaler, w, req, func() (gproto.Message, error) {
		r, err := recv()
		if err != nil {
			return nil, err
		}
		return &adaptor{r}, nil
	}, opts...)
}

func RegisterTigrisDBServerFixed(s grpc.ServiceRegistrar, srv TigrisDBServer) {
	s.RegisterService(&_TigrisDB_serviceDesc, srv)
}

func NewTigrisDBClientFixed(cc grpc.ClientConnInterface) TigrisDBClient {
	return &tigrisDBClient{cc.(*grpc.ClientConn)}
}
