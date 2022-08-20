package api

import (
	"errors"
	"net/http"

	"github.com/cyradin/search/internal/index"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
)

type Document struct {
	GUID   string                 `json:"guid"`
	Source map[string]interface{} `json:"source,omitempty"`
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

		guid, err := c.docs.Add(i, req.GUID, req.Source)
		if err != nil {
			handleErr(w, r, err)
			return
		}

		render.Status(r, http.StatusCreated)
		render.Respond(w, r, Document{GUID: guid})
	}
}

func (c *DocumentController) GetAction() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		guid := chi.URLParam(r, documentParam)
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

		source, err := c.docs.Get(i, guid)
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
		render.Respond(w, r, Document{GUID: guid, Source: source})
	}
}

func (c *DocumentController) DeleteAction() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		guid := chi.URLParam(r, documentParam)
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

		c.docs.Delete(i, guid)
		w.WriteHeader(http.StatusNoContent)
	}
}
