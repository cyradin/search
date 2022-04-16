package apiv1

import (
	"context"
	"net/http"

	"github.com/cyradin/search/pkg/logger"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func NewHandler(ctx context.Context) func(chi.Router) {
	return func(r chi.Router) {
		r.Use(
			middleware.StripSlashes,
			middleware.RequestID,
			middleware.AllowContentType("application/json"),
			middleware.SetHeader("Content-Type", "application/json"),
			bindContext(ctx),
			middleware.Logger,
			middleware.Recoverer,
		)

		r.Route("/indexes", indexHandler(ctx))
	}
}

func bindContext(appCtx context.Context) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			rctx := chi.RouteContext(r.Context())

			method := r.Method
			path := r.URL.Path
			if rctx != nil {
				if rctx.RoutePath != "" {
					path = rctx.RoutePath
				}
				if rctx.RouteMethod != "" {
					method = rctx.RouteMethod
				}
			}

			reqCtx := r.Context()
			reqCtx = logger.WithRequestMethod(reqCtx, method)
			reqCtx = logger.WithRequestRoute(reqCtx, path)
			reqCtx = logger.WithRequestID(reqCtx, r.Header.Get(middleware.RequestIDHeader))

			r = r.WithContext(reqCtx)

			next.ServeHTTP(w, r)
		}
		return http.HandlerFunc(fn)
	}
}
