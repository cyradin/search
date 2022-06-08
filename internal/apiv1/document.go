package apiv1

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/cyradin/search/internal/index"
	"github.com/cyradin/search/internal/index/entity"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
)

type Document struct {
	ID     uint32                 `json:"id"`
	Source map[string]interface{} `json:"source"`
}

type DocumentController struct {
	repo *index.Repository
}

func NewDocumentController(repo *index.Repository) *DocumentController {
	return &DocumentController{
		repo: repo,
	}
}

func (c *DocumentController) AddAction() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req Document

		if err := decodeAndValidate(r, &req); err != nil {
			handleErr(w, r, err)
			return
		}

		docs, err := c.repo.Documents(chi.URLParam(r, indexParam))
		if err != nil {
			if errors.Is(err, index.ErrIndexNotFound) {
				resp, status := NewErrResponse422(ErrResponseWithMsg(err.Error()))
				render.Status(r, status)
				render.Respond(w, r, resp)
				return
			}
			handleErr(w, r, err)
			return
		}

		id, err := docs.Add(req.ID, req.Source)
		if err != nil {
			handleErr(w, r, err)
			return
		}

		resp := struct {
			ID uint32 `json:"id"`
		}{
			ID: id,
		}
		render.Status(r, http.StatusCreated)
		render.Respond(w, r, resp)
	}
}

func (c *DocumentController) GetAction() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		idStr := chi.URLParam(r, documentParam)
		id64, err := strconv.ParseUint(idStr, 10, 32)
		id := uint32(id64)

		docs, err := c.repo.Documents(chi.URLParam(r, indexParam))
		if err != nil {
			if errors.Is(err, index.ErrIndexNotFound) {
				resp, status := NewErrResponse422(ErrResponseWithMsg(err.Error()))
				render.Status(r, status)
				render.Respond(w, r, resp)
				return
			}
			handleErr(w, r, err)
			return
		}

		doc, err := docs.Get(id)
		if err != nil {
			handleErr(w, r, err)
			return
		}

		render.Status(r, http.StatusOK)
		render.Respond(w, r, Document{ID: id, Source: doc})
	}
}

func (c *IndexController) SearchAction() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		docs, err := c.repo.Documents(chi.URLParam(r, indexParam))
		if err != nil {
			handleErr(w, r, err)
		}

		query := entity.Search{}
		if err := decodeAndValidate(r, &query); err != nil {
			resp, status := NewErrResponse400(ErrResponseWithMsg(err.Error()))
			render.Status(r, status)
			render.Respond(w, r, resp)
			return
		}

		result, err := docs.Search(query)
		if err != nil {
			handleErr(w, r, err)
			return
		}

		render.Status(r, http.StatusOK)
		render.Respond(w, r, result)
	}
}
