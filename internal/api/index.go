package api

import (
	"errors"
	"net/http"
	"time"

	"github.com/cyradin/search/internal/index"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

const (
	indexParam    = "index"
	documentParam = "document"
)

type IndexList struct {
	Items []IndexListItem `json:"items"`
}

func (l *IndexList) FromIndexes(indexes []index.Index) {
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

func (i *IndexListItem) FromIndex(item index.Index) {
	i.Name = item.Name
	i.CreatedAt = item.CreatedAt
}

type Index struct {
	Name      string        `json:"name"`
	CreatedAt time.Time     `json:"createdAt"`
	Schema    schema.Schema `json:"schema"`
}

func (i *Index) FromIndex(item index.Index) {
	i.Name = item.Name
	i.CreatedAt = item.CreatedAt
	i.Schema = item.Schema
}

type IndexAddRequest struct {
	Name   string        `json:"name" validate:"required,max=255"`
	Schema schema.Schema `json:"schema"`
}

func (r IndexAddRequest) Validate() error {
	return validation.ValidateStruct(&r,
		validation.Field(&r.Name, validation.Required, validation.Length(1, 255)),
		validation.Field(&r.Schema, validation.Required),
	)
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
		ctx := r.Context()
		indexes, err := c.repo.All(ctx)
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

func (c *IndexController) AddAction() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req IndexAddRequest

		if err := decodeAndValidate(r, &req); err != nil {
			handleErr(w, r, err)
			return
		}

		newIndex := index.New(req.Name, req.Schema)
		ctx := r.Context()
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
		ctx := r.Context()
		i, err := c.repo.Get(ctx, chi.URLParam(r, indexParam))
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
		ctx := r.Context()
		if err := c.repo.Delete(ctx, chi.URLParam(r, indexParam)); err != nil {
			handleErr(w, r, err)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

func (c *IndexController) transformIndexList(i index.Index) IndexListItem {
	return IndexListItem{
		Name:      i.Name,
		CreatedAt: i.CreatedAt,
	}
}
