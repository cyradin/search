package apiv1

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func indexHandler(ctx context.Context) func(chi.Router) {
	return func(r chi.Router) {
		r.Get("/", func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`{"indexes":[]}`))
		})
	}
}
