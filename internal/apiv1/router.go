package apiv1

import (
	"context"
	"net/http"

	"github.com/cyradin/search/internal/index"
	"github.com/cyradin/search/pkg/logger"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	jsoniter "github.com/json-iterator/go"

	"github.com/go-playground/validator/v10"
)

var (
	json = jsoniter.ConfigCompatibleWithStandardLibrary
)

func NewHandler(ctx context.Context, indexRepository *index.Repository) func(chi.Router) {
	return func(r chi.Router) {
		v := validator.New()

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
			r.Get("/", ic.ListAction())
			r.Post("/", ic.AddAction(v))
			r.Get("/{index}", ic.GetAction(v))
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

func decodeAndValidate(validator *validator.Validate, r *http.Request, data interface{}) error {
	err := json.NewDecoder(r.Body).Decode(data)
	if err != nil {
		return err
	}

	err = validator.Struct(data)
	if err != nil {
		return err
	}

	return nil
}
