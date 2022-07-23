package api

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/cyradin/search/internal/index"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
)

type Document struct {
	ID     uint32                 `json:"id"`
	Source map[string]interface{} `json:"source"`
}

type DocumentController struct {
	repo *index.Repository
	docs *index.Documents
}

func NewDocumentController(repo *index.Repository, docs *index.Documents) *DocumentController {
	return &DocumentController{
		repo: repo,
		docs: docs,
	}
}

func (c *DocumentController) AddAction() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req Document

		if err := decodeAndValidate(r, &req); err != nil {
			handleErr(w, r, err)
			return
		}

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

		err = c.docs.Add(i, req.ID, req.Source)
		if err != nil {
			handleErr(w, r, err)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}

func (c *DocumentController) GetAction() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		idStr := chi.URLParam(r, documentParam)
		id64, err := strconv.ParseUint(idStr, 10, 32)
		id := uint32(id64)

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

		source, err := c.docs.Get(i, id)
		if err != nil {
			if errors.Is(err, index.ErrDocNotFound) {
				resp, status := NewErrResponse404(ErrResponseWithMsg(err.Error()))
				render.Status(r, status)
				render.Respond(w, r, resp)
				return
			}

			handleErr(w, r, err)
			return
		}

		render.Status(r, http.StatusOK)
		render.Respond(w, r, Document{ID: id, Source: source})
	}
}

func (c *DocumentController) DeleteAction() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		idStr := chi.URLParam(r, documentParam)
		id64, err := strconv.ParseUint(idStr, 10, 32)
		id := uint32(id64)

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

		c.docs.Delete(i, id)
		w.WriteHeader(http.StatusNoContent)
	}
}

func (c *DocumentController) SearchAction() http.HandlerFunc {
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
