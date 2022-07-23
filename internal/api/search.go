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
	docs *index.Documents
}

func NewSearchController(repo *index.Repository, docs *index.Documents) *SearchController {
	return &SearchController{
		repo: repo,
		docs: docs,
	}
}

func (c *SearchController) SearchAction() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		i, err := c.repo.Get(chi.URLParam(r, indexParam))
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

		result, err := c.docs.Search(ctx, i, query)
		if err != nil {
			handleErr(w, r, err)
			return
		}

		render.Status(r, http.StatusOK)
		render.Respond(w, r, result)
	}
}
