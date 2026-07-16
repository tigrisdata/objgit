package svc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"connectrpc.com/connect"
	"connectrpc.com/validate"
	"connectrpc.com/vanguard"
	"github.com/mdigger/rpclog"
	metav1 "github.com/tigrisdata/objgit/cmd/objgitd/svc/meta/v1"
	"github.com/tigrisdata/objgit/gen/tigrisdata/objgit/meta/v1/metav1connect"
)

func Route(ctx context.Context, lg *slog.Logger) (http.Handler, error) {
	mux := http.NewServeMux()
	logger := rpclog.New(lg)

	errs := []error{}
	var svcs []*vanguard.Service

	{
		metaSvc, err := metav1.New(ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("can't construct metav1 service: %w", err))
		}

		path, handler := metav1connect.NewWhoAmIServiceHandler(
			metaSvc,
			connect.WithInterceptors(logger, validate.NewInterceptor()),
		)
		mux.Handle(path, handler)

		svc := vanguard.NewService(path, handler)
		svcs = append(svcs, svc)
	}

	transcoder, err := vanguard.NewTranscoder(svcs)
	if err != nil {
		errs = append(errs, fmt.Errorf("can't create vanguard transcoder: %w", err))
	}

	mux.Handle("/", transcoder)

	if len(errs) != 0 {
		return nil, fmt.Errorf("can't build API server: %w", errors.Join(errs...))
	}

	return mux, nil
}
