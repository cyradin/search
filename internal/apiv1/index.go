package apiv1

import (
	"errors"
	"net/http"
	"time"

	"github.com/cyradin/search/internal/entity"
	"github.com/cyradin/search/internal/index"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/go-playground/validator/v10"
)

const (
	indexParam    = "index"
	documentParam = "document"
)

type IndexList struct {
	Items []IndexListItem `json:"items"`
}

func (l *IndexList) FromIndexes(indexes []entity.Index) {
	l.Items = make([]IndexListItem, len(indexes))
	for i, item := range indexes {
		listItem := IndexListItem{}
		listItem.FromIndex(item)
		l.Items[i] = listItem
	}
}

type IndexListItem struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"createdAt"`
}

func (i *IndexListItem) FromIndex(item entity.Index) {
	i.Name = item.Name
	i.CreatedAt = item.CreatedAt
}

type Index struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"createdAt"`
	Schema    Schema    `json:"schema"`
}

type Document struct {
	ID     string                 `json:"id"`
	Source map[string]interface{} `json:"source"`
}

func (i *Index) FromIndex(item entity.Index) {
	i.Name = item.Name
	i.CreatedAt = item.CreatedAt
	i.Schema.FromSchema(item.Schema)
}

type IndexAddRequest struct {
	Name   string `json:"name" validate:"required,max=255"`
	Schema Schema `json:"schema"`
}

type IndexController struct {
	repo *index.Repository
}

func NewIndexController(repo *index.Repository) *IndexController {
	return &IndexController{
		repo: repo,
	}
}

func (c *IndexController) ListAction() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		indexes, err := c.repo.All()
		if err != nil {
			handleErr(w, r, err)
			return
		}
		resp := IndexList{}
		resp.FromIndexes(indexes)

		render.Status(r, http.StatusOK)
		render.Respond(w, r, resp)
	}
}

func (c *IndexController) AddAction(validator *validator.Validate) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		var req IndexAddRequest

		if err := decodeAndValidate(validator, r, &req); err != nil {
			handleErr(w, r, err)
			return
		}

		newIndex := entity.NewIndex(req.Name, req.Schema.ToSchema())

		err := c.repo.Add(ctx, newIndex)
		if err != nil {
			if errors.Is(err, index.ErrIndexAlreadyExists) {
				resp, status := NewErrResponse422(ErrResponseWithMsg(err.Error()))
				render.Status(r, status)
				render.Respond(w, r, resp)
				return
			}
			handleErr(w, r, err)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}

func (c *IndexController) GetAction() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		i, err := c.repo.Get(chi.URLParam(r, indexParam))
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

		resp := Index{}
		resp.FromIndex(i)

		render.Status(r, http.StatusOK)
		render.Respond(w, r, resp)
	}
}

func (c *IndexController) DeleteAction() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := c.repo.Delete(chi.URLParam(r, indexParam)); err != nil {
			handleErr(w, r, err)
			return
		}

		render.Status(r, http.StatusOK)
	}
}

func (c *IndexController) SearchAction(validator *validator.Validate) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		docs, err := c.repo.Documents(chi.URLParam(r, indexParam))
		if err != nil {
			handleErr(w, r, err)
		}

		query := entity.Search{}
		if err := decodeAndValidate(validator, r, &query); err != nil {
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

func (c *IndexController) DocumentAddAction(validator *validator.Validate) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req Document

		if err := decodeAndValidate(validator, r, &req); err != nil {
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
			ID string `json:"id"`
		}{
			ID: id,
		}
		render.Status(r, http.StatusCreated)
		render.Respond(w, r, resp)
	}
}

func (c *IndexController) DocumentGetAction() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, documentParam)

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

		resp := Document{
			ID:     id,
			Source: doc,
		}

		render.Status(r, http.StatusOK)
		render.Respond(w, r, resp)
	}
}

func (c *IndexController) transformIndexList(i entity.Index) IndexListItem {
	return IndexListItem{
		Name:      i.Name,
		CreatedAt: i.CreatedAt,
	}
}
