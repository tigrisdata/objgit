package metav1

import (
	"context"

	"connectrpc.com/connect"
	metav1 "github.com/tigrisdata/objgit/gen/tigrisdata/objgit/meta/v1"
	"github.com/tigrisdata/objgit/gen/tigrisdata/objgit/meta/v1/metav1connect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct{}

func New(ctx context.Context) (metav1connect.WhoAmIServiceHandler, error) {
	return &Server{}, nil
}

func (s *Server) WhoAmI(ctx context.Context, req *connect.Request[metav1.WhoAmIRequest]) (*connect.Response[metav1.WhoAmIResponse], error) {
	resp := connect.NewResponse(&metav1.WhoAmIResponse{
		AccessKeyId: "TODO: not implemented",
		Time:        timestamppb.Now(),
	})

	return resp, nil
}
