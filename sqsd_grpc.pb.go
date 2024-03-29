// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.19.4
// source: sqsd.proto

package sqsd

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// MonitoringServiceClient is the client API for MonitoringService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type MonitoringServiceClient interface {
	CurrentWorkings(ctx context.Context, in *CurrentWorkingsRequest, opts ...grpc.CallOption) (*CurrentWorkingsResponse, error)
}

type monitoringServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewMonitoringServiceClient(cc grpc.ClientConnInterface) MonitoringServiceClient {
	return &monitoringServiceClient{cc}
}

func (c *monitoringServiceClient) CurrentWorkings(ctx context.Context, in *CurrentWorkingsRequest, opts ...grpc.CallOption) (*CurrentWorkingsResponse, error) {
	out := new(CurrentWorkingsResponse)
	err := c.cc.Invoke(ctx, "/sqsd.MonitoringService/CurrentWorkings", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MonitoringServiceServer is the server API for MonitoringService service.
// All implementations must embed UnimplementedMonitoringServiceServer
// for forward compatibility
type MonitoringServiceServer interface {
	CurrentWorkings(context.Context, *CurrentWorkingsRequest) (*CurrentWorkingsResponse, error)
	mustEmbedUnimplementedMonitoringServiceServer()
}

// UnimplementedMonitoringServiceServer must be embedded to have forward compatible implementations.
type UnimplementedMonitoringServiceServer struct {
}

func (UnimplementedMonitoringServiceServer) CurrentWorkings(context.Context, *CurrentWorkingsRequest) (*CurrentWorkingsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CurrentWorkings not implemented")
}
func (UnimplementedMonitoringServiceServer) mustEmbedUnimplementedMonitoringServiceServer() {}

// UnsafeMonitoringServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to MonitoringServiceServer will
// result in compilation errors.
type UnsafeMonitoringServiceServer interface {
	mustEmbedUnimplementedMonitoringServiceServer()
}

func RegisterMonitoringServiceServer(s grpc.ServiceRegistrar, srv MonitoringServiceServer) {
	s.RegisterService(&MonitoringService_ServiceDesc, srv)
}

func _MonitoringService_CurrentWorkings_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CurrentWorkingsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MonitoringServiceServer).CurrentWorkings(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/sqsd.MonitoringService/CurrentWorkings",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MonitoringServiceServer).CurrentWorkings(ctx, req.(*CurrentWorkingsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// MonitoringService_ServiceDesc is the grpc.ServiceDesc for MonitoringService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var MonitoringService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "sqsd.MonitoringService",
	HandlerType: (*MonitoringServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CurrentWorkings",
			Handler:    _MonitoringService_CurrentWorkings_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "sqsd.proto",
}
