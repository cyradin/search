package query

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cyradin/search/internal/index/field"
	"github.com/cyradin/search/internal/index/schema"
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
	q := `{
		"should": [
			{
				"term": {
					"field": {
						"query": "should1"
					}
				}
			},
			{
				"term": {
					"field": {
						"query": "should2"
					}
				}
			}
		],
		"must": [
			{
				"term": {
					"field": {
						"query": "must"
					}
				}
			}
		],
		"filter": [
			{
				"term": {
					"field": {
						"query": "filter"
					}
				}
			}
		]
	}`

	query := make(map[string]interface{})
	err := json.Unmarshal([]byte(q), &query)
	if err != nil {
		panic(err)
	}

	for _, cnt := range counts {
		f, err := field.New(field.FieldData{Type: schema.TypeKeyword})
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

		ctx := withFields(context.Background(), Fields{
			"field": f,
		})

		b.Run(fmt.Sprintf("doc_cnt_%d", cnt), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				bq, err := newBoolQuery(ctx, query)
				if err != nil {
					panic(err)
				}
				if parallel {
					bq.parallel = true
				}

				result, err := bq.exec(ctx)
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

func Test_newBoolQuery(t *testing.T) {
	t.Run("must not return error if request is an empty object", func(t *testing.T) {
		query := "{}"
		req, err := decodeQuery(query)
		require.NoError(t, err)

		q, err := newBoolQuery(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, q)
	})
	t.Run("must return error if request contains extra keys", func(t *testing.T) {
		query := `{
				"should": [],
				"field": []
			}`
		req, err := decodeQuery(query)
		require.NoError(t, err)

		q, err := newBoolQuery(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, q)
	})
	t.Run("must return error if request field is not an array", func(t *testing.T) {
		query := `{
				"should": {
					"query": "hello"
				}
			}`
		req, err := decodeQuery(query)
		require.NoError(t, err)

		q, err := newBoolQuery(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, q)
	})
	t.Run("must not return error if request field is an empty array", func(t *testing.T) {
		query := `{
				"should": []
			}`
		req, err := decodeQuery(query)
		require.NoError(t, err)

		q, err := newBoolQuery(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, q)
	})
	t.Run("must return error if request field contains invalid subquery", func(t *testing.T) {
		query := `{
				"should": [
					{
						"term": 1
					}
				]
			}`
		req, err := decodeQuery(query)
		require.NoError(t, err)

		q, err := newBoolQuery(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, q)
	})
	t.Run("must not return error if request is a valid query", func(t *testing.T) {
		query := `{
				"should": [
					{
						"term": {
							"query": "value"
						}
					}
				],
				"must": [
					{
						"bool": {
							"should": [
								{
									"term": {
										"query": "value"
									}
								}
							]
						}
					}
				],
				"filter": [
					{
						"bool": {}
					}
				]
			}`
		req, err := decodeQuery(query)
		require.NoError(t, err)

		q, err := newBoolQuery(context.Background(), req)
		require.Error(t, err)
		require.Nil(t, q)
	})
}

func Test_boolQuery_exec(t *testing.T) {
	f1 := field.NewBool()
	f1.Add(1, true)
	f1.Add(2, false)

	f2 := field.NewAll()
	f2.Add(1, true)
	f2.Add(2, false)
	f2.Add(3, true)

	ctx := withFields(context.Background(), map[string]field.Field{"field": f1, field.AllField: f2})

	t.Run("must return all documents for empty query", func(t *testing.T) {
		query, err := decodeQuery(`{}`)
		require.NoError(t, err)
		bq, err := newBoolQuery(ctx, query)
		require.NoError(t, err)
		result, err := bq.exec(ctx)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.ElementsMatch(t, []uint32{1, 2, 3}, result.Docs().ToArray())
	})
	t.Run("must return documents for should query if documents match at least one query", func(t *testing.T) {
		query, err := decodeQuery(`
			{
				"should": [
					{
						"term": {
							"field": {
								"query": true
							}
						}
					},
					{
						"term": {
							"field": {
								"query": false
							}
						}
					}
				]
			}
			`)
		require.NoError(t, err)
		bq, err := newBoolQuery(ctx, query)
		require.NoError(t, err)
		result, err := bq.exec(ctx)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.ElementsMatch(t, []uint32{1, 2}, result.Docs().ToArray())
	})
	t.Run("must return no documents for must query if no documents match all queries", func(t *testing.T) {
		query, err := decodeQuery(`
			{
				"must": [
					{
						"term": {
							"field": {
								"query": true
							}
						}
					},
					{
						"term": {
							"field": {
								"query": false
							}
						}
					}
				]
			}
			`)
		require.NoError(t, err)
		bq, err := newBoolQuery(ctx, query)
		require.NoError(t, err)
		result, err := bq.exec(ctx)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.ElementsMatch(t, []uint32{}, result.Docs().ToArray())
	})
	t.Run("must return documents for must query if some documents match all queries", func(t *testing.T) {
		query, err := decodeQuery(`
			{
				"must": [
					{
						"term": {
							"field": {
								"query": true
							}
						}
					},
					{
						"term": {
							"field": {
								"query": true
							}
						}
					}
				]
			}
			`)
		require.NoError(t, err)
		bq, err := newBoolQuery(ctx, query)
		require.NoError(t, err)
		result, err := bq.exec(ctx)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.ElementsMatch(t, []uint32{1}, result.Docs().ToArray())
	})
	t.Run("must return no documents for must+should query if no documents match all queries", func(t *testing.T) {
		query, err := decodeQuery(`
			{
				"should": [
					{
						"term": {
							"field": {
								"query": true
							}
						}
					},
					{
						"term": {
							"field": {
								"query": false
							}
						}
					}
				],
				"must": [
					{
						"term": {
							"field": {
								"query": true
							}
						}
					},
					{
						"term": {
							"field": {
								"query": false
							}
						}
					}
				]
			}
			`)
		require.NoError(t, err)
		bq, err := newBoolQuery(ctx, query)
		require.NoError(t, err)
		result, err := bq.exec(ctx)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.ElementsMatch(t, []uint32{}, result.Docs().ToArray())
	})
	t.Run("must return no documents for filter query if no documents match all queries", func(t *testing.T) {
		query, err := decodeQuery(`
			{
				"filter": [
					{
						"term": {
							"field": {
								"query": true
							}
						}
					},
					{
						"term": {
							"field": {
								"query": false
							}
						}
					}
				]
			}
			`)
		require.NoError(t, err)
		bq, err := newBoolQuery(ctx, query)
		require.NoError(t, err)
		result, err := bq.exec(ctx)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.ElementsMatch(t, []uint32{}, result.Docs().ToArray())
	})
	t.Run("must return documents for filter query if some documents match all queries", func(t *testing.T) {
		query, err := decodeQuery(`
			{
				"filter": [
					{
						"term": {
							"field": {
								"query": true
							}
						}
					},
					{
						"term": {
							"field": {
								"query": true
							}
						}
					}
				]
			}
			`)
		require.NoError(t, err)
		bq, err := newBoolQuery(ctx, query)
		require.NoError(t, err)
		result, err := bq.exec(ctx)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.ElementsMatch(t, []uint32{1}, result.Docs().ToArray())
	})
	t.Run("must return no documents for filter+should query if no documents match all queries", func(t *testing.T) {
		query, err := decodeQuery(`
			{
				"should": [
					{
						"term": {
							"field": {
								"query": true
							}
						}
					},
					{
						"term": {
							"field": {
								"query": false
							}
						}
					}
				],
				"must": [
					{
						"term": {
							"field": {
								"query": true
							}
						}
					},
					{
						"term": {
							"field": {
								"query": false
							}
						}
					}
				]
			}
			`)
		require.NoError(t, err)
		bq, err := newBoolQuery(ctx, query)
		require.NoError(t, err)
		result, err := bq.exec(ctx)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.ElementsMatch(t, []uint32{}, result.Docs().ToArray())
	})
}
