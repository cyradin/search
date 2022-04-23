package apiv1

import (
	"fmt"
	"net/http"
	"time"

	"github.com/cyradin/search/internal/index"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/go-playground/validator/v10"
)

type IndexList struct {
	Items []IndexListItem `json:"items"`
}

func (l *IndexList) FromIndexes(indexes []*index.Index) {
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

func (i *IndexListItem) FromIndex(item *index.Index) {
	i.Name = item.Name
	i.CreatedAt = item.CreatedAt
}

type Index struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"createdAt"`
	Schema    Schema    `json:"schema"`
}

func (i *Index) FromIndex(item *index.Index) {
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
			// @todo handle error properly
			w.WriteHeader(500)
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
			// @todo hande err properly
			fmt.Println(err)
			w.WriteHeader(http.StatusUnprocessableEntity)
			return
		}

		newIndex := index.New(ctx, req.Name, req.Schema.ToSchema())

		err := c.repo.Add(ctx, newIndex)
		if err != nil {
			// @todo hande err properly
			fmt.Println(err)
			w.WriteHeader(http.StatusUnprocessableEntity)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}

func (c *IndexController) GetAction() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		index, err := c.repo.Get(chi.URLParam(r, "index"))
		if err != nil {
			// @todo hande err properly
			fmt.Println(err)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		resp := Index{}
		resp.FromIndex(index)

		render.Status(r, http.StatusOK)
		render.Respond(w, r, resp)
	}
}

func (c *IndexController) DeleteAction() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := c.repo.Delete(chi.URLParam(r, "index")); err != nil {
			// @todo hande err properly
			fmt.Println(err)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		render.Status(r, http.StatusOK)
	}
}

func (c *IndexController) transformIndexList(i *index.Index) IndexListItem {
	return IndexListItem{
		Name:      i.Name,
		CreatedAt: i.CreatedAt,
	}
}
