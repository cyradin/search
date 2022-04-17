package apiv1

import (
	"fmt"
	"net/http"
	"time"

	"github.com/cyradin/search/internal/index"
	"github.com/go-chi/render"
	"github.com/go-playground/validator/v10"
)

type IndexList struct {
	Items []IndexListItem `json:"items"`
}

type IndexListItem struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"createdAt"`
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

		items := make([]IndexListItem, len(indexes))
		for i, index := range indexes {
			items[i] = c.transformIndexList(index)
		}

		resp := IndexList{
			Items: items,
		}

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

		err := c.repo.Add(newIndex)
		if err != nil {
			// @todo hande err properly
			fmt.Println(err)
			w.WriteHeader(http.StatusUnprocessableEntity)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}

func (c *IndexController) transformIndexList(i *index.Index) IndexListItem {
	return IndexListItem{
		Name:      i.Name,
		CreatedAt: i.CreatedAt,
	}
}
