package api

import (
	"errors"
	"net/http"

	"github.com/cyradin/search/internal/index"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
)

type SearchController struct {
	repo *index.Repository
}

func NewSearchController(repo *index.Repository) *SearchController {
	return &SearchController{
		repo: repo,
	}
}

func (c *SearchController) SearchAction() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		i, err := c.repo.Get(ctx, chi.URLParam(r, indexParam))
		if err != nil {
			if errors.Is(err, index.ErrIndexNotFound) {
				resp, status := NewErrResponse404(ErrResponseWithMsg(err.Error()))
				render.Status(r, status)
				render.Respond(w, r, resp)
				return
			}
			handleErr(w, r, err)
			return
		}

		query := index.Search{}
		if err := decodeAndValidate(r, &query); err != nil {
			resp, status := NewErrResponse400(ErrResponseWithMsg(err.Error()))
			render.Status(r, status)
			render.Respond(w, r, resp)
			return
		}

		result, err := i.Search(ctx, query)
		if err != nil {
			handleErr(w, r, err)
			return
		}

		render.Status(r, http.StatusOK)
		render.Respond(w, r, result)
	}
}
