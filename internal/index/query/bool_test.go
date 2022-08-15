package query

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
)

func Benchmark_boolQuery_sync(b *testing.B) {
	counts := []int{1, 10, 100, 1000, 10000, 100000, 1000000}
	bench_boolQuery(b, counts, false)
}

func Benchmark_boolQuery_parallel(b *testing.B) {
	counts := []int{1, 10, 100, 1000, 10000, 100000, 1000000}
	bench_boolQuery(b, counts, true)
}

func bench_boolQuery(b *testing.B, counts []int, parallel bool) {
	query := []byte(`{
		"type": "bool",
		"should": [
			{
				"type": "term",
				"field": "field",
				"query": "should1"
			},
			{
				"type": "term",
				"field": "field",
				"query": "should2"
			}
		],
		"must": [
			{
				"type": "term",
				"field": "field",
				"query": "must"
			}
		],
		"filter": [
			{
				"type": "term",
				"field": "field",
				"query": "filter"
			}
		]
	}`)

	for _, cnt := range counts {
		f, err := field.New(schema.TypeKeyword)
		if err != nil {
			panic(err)
		}

		for i := 0; i < cnt; i++ {
			// every 10th i will match bool query
			if i%2 == 0 {
				f.Add(uint32(i), "must")
			}
			if i%5 == 0 {
				f.Add(uint32(i), "filter")
			}

			f.Add(uint32(i), "should1")
			if i%3 == 0 {
				f.Add(uint32(i), "should2")
			}
		}

		b.Run(fmt.Sprintf("doc_cnt_%d", cnt), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				q, err := Build(query)

				bq := q.(*BoolQuery)
				if err != nil {
					panic(err)
				}
				if parallel {
					bq.Parallel = true
				}

				result, err := bq.Exec(context.Background(), Fields{
					"field": f,
				})
				if err != nil {
					panic(err)
				}

				if cnt >= 10 && int(result.Docs().GetCardinality()) != cnt/10 {
					panic("invalid query result")
				}
			}
		})
	}
}

func Test_BoolQuery_Validate(t *testing.T) {
	t.Run("must not return error if request is an empty object", func(t *testing.T) {
		query := new(BoolQuery)
		mustUnmarshal(t, `{}`, query)

		err := validation.Validate(query)
		require.NoError(t, err)
	})

	t.Run("must not return error if request field is an empty array", func(t *testing.T) {
		query := new(BoolQuery)
		mustUnmarshal(t, `{
				"should": []
			}`, query)

		err := validation.Validate(query)
		require.NoError(t, err)
	})

	t.Run("must return error if request field contains invalid subquery", func(t *testing.T) {
		query := new(BoolQuery)

		dec := jsoniter.NewDecoder(bytes.NewBuffer([]byte(`{
			"should": [
				{
					"term": 1
				}
			]
		}`)))
		dec.UseNumber()
		err := dec.Decode(query)
		require.Error(t, err)
	})
	t.Run("must not return error if request is a valid query", func(t *testing.T) {
		query := new(BoolQuery)
		mustUnmarshal(t, `{
			"should": [
				{
					"type": "term",
					"field": "field",
					"query": "query"
				}
			],
			"must": [
				{
					"type": "bool",
					"should": [
						{
							"type": "term",
							"field": "field",
							"query": "query"
						}
					]
				}
			],
			"filter": [
				{
					"type": "bool"
				}
			]
		}`, query)

		err := validation.Validate(query)
		require.NoError(t, err)
	})
}

func Test_BoolQuery_Exec(t *testing.T) {
	f1, err := field.New(schema.TypeBool)
	require.NoError(t, err)
	f1.Add(1, true)
	f1.Add(2, false)

	f2, err := field.New(schema.TypeAll)
	require.NoError(t, err)
	f2.Add(1, true)
	f2.Add(2, false)
	f2.Add(3, true)

	t.Run("must return all documents for empty query", func(t *testing.T) {
		query := new(BoolQuery)
		mustUnmarshal(t, `{}`, query)

		result, err := query.Exec(context.Background(), Fields{"field": f1, field.AllField: f2})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.ElementsMatch(t, []uint32{1, 2, 3}, result.Docs().ToArray())
	})
	t.Run("must return documents for should query if documents match at least one query", func(t *testing.T) {
		query := new(BoolQuery)
		mustUnmarshal(t, `{
			"should": [
				{
					"type": "term",
					"field": "field",
					"query": true
				},
				{
					"type": "term",
					"field": "field",
					"query": false
				}
			]
		}`, query)

		result, err := query.Exec(context.Background(), Fields{"field": f1, field.AllField: f2})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.ElementsMatch(t, []uint32{1, 2}, result.Docs().ToArray())
	})
	t.Run("must return no documents for must query if no documents match all queries", func(t *testing.T) {
		query := new(BoolQuery)
		mustUnmarshal(t, `{
			"must": [
				{
					"type": "term",
					"field": "field",
					"query": true
				},
				{
					"type": "term",
					"field": "field",
					"query": false
				}
			]
		}`, query)

		result, err := query.Exec(context.Background(), Fields{"field": f1, field.AllField: f2})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.ElementsMatch(t, []uint32{}, result.Docs().ToArray())
	})
	t.Run("must return documents for must query if some documents match all queries", func(t *testing.T) {
		query := new(BoolQuery)
		mustUnmarshal(t, `{
			"must": [
				{
					"type": "term",
					"field": "field",
					"query": true
				},
				{
					"type": "term",
					"field": "field",
					"query": true
				}
			]
		}`, query)

		result, err := query.Exec(context.Background(), Fields{"field": f1, field.AllField: f2})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.ElementsMatch(t, []uint32{1}, result.Docs().ToArray())
	})
	t.Run("must return no documents for must+should query if no documents match all queries", func(t *testing.T) {
		query := new(BoolQuery)
		mustUnmarshal(t, `{
			"must": [
				{
					"type": "term",
					"field": "field",
					"query": true
				},
				{
					"type": "term",
					"field": "field",
					"query": false
				}
			],
			"should": [
				{
					"type": "term",
					"field": "field",
					"query": true
				},
				{
					"type": "term",
					"field": "field",
					"query": false
				}
			]
		}`, query)

		result, err := query.Exec(context.Background(), Fields{"field": f1, field.AllField: f2})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.ElementsMatch(t, []uint32{}, result.Docs().ToArray())
	})
	t.Run("must return no documents for filter query if no documents match all queries", func(t *testing.T) {
		query := new(BoolQuery)
		mustUnmarshal(t, `{
			"filter": [
				{
					"type": "term",
					"field": "field",
					"query": true
				},
				{
					"type": "term",
					"field": "field",
					"query": false
				}
			]
		}`, query)

		result, err := query.Exec(context.Background(), Fields{"field": f1, field.AllField: f2})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.ElementsMatch(t, []uint32{}, result.Docs().ToArray())
	})
	t.Run("must return documents for filter query if some documents match all queries", func(t *testing.T) {
		query := new(BoolQuery)
		mustUnmarshal(t, `{
			"filter": [
				{
					"type": "term",
					"field": "field",
					"query": true
				},
				{
					"type": "term",
					"field": "field",
					"query": true
				}
			]
		}`, query)

		result, err := query.Exec(context.Background(), Fields{"field": f1, field.AllField: f2})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.ElementsMatch(t, []uint32{1}, result.Docs().ToArray())
	})
	t.Run("must return no documents for filter+should query if no documents match all queries", func(t *testing.T) {
		query := new(BoolQuery)
		mustUnmarshal(t, `{
			"filter": [
				{
					"type": "term",
					"field": "field",
					"query": true
				},
				{
					"type": "term",
					"field": "field",
					"query": false
				}
			],
			"should": [
				{
					"type": "term",
					"field": "field",
					"query": true
				},
				{
					"type": "term",
					"field": "field",
					"query": false
				}
			]
		}`, query)

		result, err := query.Exec(context.Background(), Fields{"field": f1, field.AllField: f2})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.ElementsMatch(t, []uint32{}, result.Docs().ToArray())
	})
}
