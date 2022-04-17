package apiv1

import (
	"fmt"
	"net/http"
	"time"

	"github.com/cyradin/search/internal/index"
	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
	"github.com/go-playground/validator/v10"
)

type IndexList struct {
	Items []IndexListItem `json:"items"`
}

type IndexListItem struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"createdAt"`
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
		data, err := json.Marshal(resp)
		if err != nil {
			// @todo handle error properly
			w.WriteHeader(500)
			return
		}

		w.Write(data)
	}
}

type IndexAddRequest struct {
	Name   string `json:"name" validate:"required,max=255"`
	Schema Schema `json:"schema"`
}

type Schema struct {
	Fields map[string]SchemaField `json:"fields" validate:"required"`
}

type SchemaField struct {
	Type     string `json:"type"`
	Required bool   `json:"required"`

	Fields map[string]SchemaField `json:"fields"`
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

		newIndex := index.New(ctx, req.Name, c.transformReqSchema(req.Schema))

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

func (c *IndexController) transformReqSchema(s Schema) schema.Schema {
	res := schema.Schema{
		Fields: make([]schema.Field, 0, len(s.Fields)),
	}

	for name, f := range s.Fields {
		res.Fields = append(res.Fields, c.transformReqSchemaField(name, f))
	}

	return res
}

func (c *IndexController) transformReqSchemaField(name string, f SchemaField) schema.Field {
	result := schema.Field{
		Name:     name,
		Type:     field.Type(f.Type),
		Required: f.Required,
	}

	if len(f.Fields) > 0 {
		result.Children = make([]schema.Field, 0, len(f.Fields))
		for name, child := range f.Fields {
			result.Children = append(result.Children, c.transformReqSchemaField(name, child))
		}
	}

	return result
}

func (c *IndexController) transformIndexList(i *index.Index) IndexListItem {
	return IndexListItem{
		Name:      i.Name,
		CreatedAt: i.CreatedAt,
	}
}
