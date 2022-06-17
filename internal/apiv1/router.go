package apiv1

import (
	"context"
	"fmt"
	"net/http"

	"github.com/cyradin/search/internal/index"
	"github.com/cyradin/search/internal/logger"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	jsoniter "github.com/json-iterator/go"
)

func NewHandler(ctx context.Context, indexRepository *index.Repository) func(chi.Router) {
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

		r.Route("/indexes", func(r chi.Router) {
			ic := NewIndexController(indexRepository)
			dc := NewDocumentController(indexRepository)
			r.Get("/", ic.ListAction())
			r.Post("/", ic.AddAction())

			r.Route("/{"+indexParam+"}", func(r chi.Router) {
				r.Get("/", ic.GetAction())
				r.Delete("/", ic.DeleteAction())
				r.Post("/search", ic.SearchAction())
				r.Route("/documents", func(r chi.Router) {
					r.Post("/", dc.AddAction())
					r.Get("/{"+documentParam+"}", dc.GetAction())
				})
			})

		})
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

func decodeAndValidate(r *http.Request, data interface{}) error {
	dec := jsoniter.NewDecoder(r.Body)
	dec.UseNumber()
	err := dec.Decode(data)
	if err != nil {
		return fmt.Errorf("%w: %s", errJsonUnmarshal, err.Error())
	}

	err = validation.Validate(data)
	if err != nil {
		return err
	}

	return nil
}
