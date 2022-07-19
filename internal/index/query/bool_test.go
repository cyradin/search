package query

import (
	"context"
	"testing"

	"github.com/cyradin/search/internal/index/field"
	"github.com/stretchr/testify/require"
)

func Test_boolQuery(t *testing.T) {
	t.Run("newBoolQuery", func(t *testing.T) {
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
	})

	t.Run("exec", func(t *testing.T) {
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
	})

	t.Run("exec", func(t *testing.T) {
		f1 := field.NewNumeric[int32]()
		f1.Add(1, 1)
		f1.Add(1, 3)
		f1.Add(2, 2)

		f2 := field.NewAll()
		f2.Add(1, 1)
		f2.Add(2, 2)
		ctx := withFields(context.Background(), map[string]field.Field{"field": f1, field.AllField: f2})

		t.Run("must return all docs for empty query", func(t *testing.T) {
			req, err := decodeQuery(`{}`)
			require.NoError(t, err)
			require.NoError(t, err)

			bq, err := newBoolQuery(ctx, req)
			require.NoError(t, err)

			result, err := bq.exec(ctx)
			require.NoError(t, err)

			require.EqualValues(t, []uint32{1, 2}, result.Docs().ToArray())
		})
		t.Run("must return union of should queries", func(t *testing.T) {
			req, err := decodeQuery(`
			{
				"should": [
					{
						"term": {
							"field": {
								"query": 1
							}
						}
					},
					{
						"term": {
							"field": {
								"query": 2
							}
						}
					}
				]
			}
			`)
			require.NoError(t, err)
			require.NoError(t, err)

			bq, err := newBoolQuery(ctx, req)
			require.NoError(t, err)

			result, err := bq.exec(ctx)
			require.NoError(t, err)

			require.EqualValues(t, []uint32{1, 2}, result.Docs().ToArray())
		})
		t.Run("must return intersection of must queries", func(t *testing.T) {
			t.Run("empty intersection", func(t *testing.T) {
				req, err := decodeQuery(`
				{
					"must": [
						{
							"term": {
								"field": {
									"query": 1
								}
							}
						},
						{
							"term": {
								"field": {
									"query": 2
								}
							}
						}
					]
				}
				`)
				require.NoError(t, err)
				require.NoError(t, err)

				bq, err := newBoolQuery(ctx, req)
				require.NoError(t, err)

				result, err := bq.exec(ctx)
				require.NoError(t, err)

				require.EqualValues(t, []uint32{}, result.Docs().ToArray())
			})
			t.Run("not empty intersection", func(t *testing.T) {
				req, err := decodeQuery(`
				{
					"must": [
						{
							"term": {
								"field": {
									"query": 1
								}
							}
						},
						{
							"term": {
								"field": {
									"query": 3
								}
							}
						}
					]
				}
				`)
				require.NoError(t, err)
				require.NoError(t, err)

				bq, err := newBoolQuery(ctx, req)
				require.NoError(t, err)

				result, err := bq.exec(ctx)
				require.NoError(t, err)

				require.EqualValues(t, []uint32{1}, result.Docs().ToArray())
			})
		})
		t.Run("must return intersection of filter queries", func(t *testing.T) {
			t.Run("empty intersection", func(t *testing.T) {
				req, err := decodeQuery(`
				{
					"filter": [
						{
							"term": {
								"field": {
									"query": 1
								}
							}
						},
						{
							"term": {
								"field": {
									"query": 2
								}
							}
						}
					]
				}
				`)
				require.NoError(t, err)
				require.NoError(t, err)

				bq, err := newBoolQuery(ctx, req)
				require.NoError(t, err)

				result, err := bq.exec(ctx)
				require.NoError(t, err)

				require.EqualValues(t, []uint32{}, result.Docs().ToArray())
			})
			t.Run("not empty intersection", func(t *testing.T) {
				req, err := decodeQuery(`
				{
					"filter": [
						{
							"term": {
								"field": {
									"query": 1
								}
							}
						},
						{
							"term": {
								"field": {
									"query": 3
								}
							}
						}
					]
				}
				`)
				require.NoError(t, err)
				require.NoError(t, err)

				bq, err := newBoolQuery(ctx, req)
				require.NoError(t, err)

				result, err := bq.exec(ctx)
				require.NoError(t, err)

				require.EqualValues(t, []uint32{1}, result.Docs().ToArray())
			})
		})
	})
}
