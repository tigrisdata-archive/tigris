// Copyright 2022-2023 Tigris Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package search

type MultiSearchSearchesParameter struct {
	Searches []MultiSearchCollectionParameters `json:"searches"`
}

// MultiSearchCollectionParameters defines model for MultiSearchCollectionParameters.
type MultiSearchCollectionParameters struct {
	// Embedded struct due to allOf(#/components/schemas/MultiSearchParameters)
	MultiSearchParameters `yaml:",inline"`
	// Embedded fields due to inline allOf schema
	// The collection to search in.
	Collection string `json:"collection"`
}

type MultiSearchParameters struct {
	// The duration (in seconds) that determines how long the search query is cached.  This value can be set on a per-query basis. Default: 60.
	CacheTtl *int `json:"cache_ttl,omitempty"`

	// If the number of results found for a specific query is less than this number, Typesense will attempt to drop the tokens in the query until enough results are found. Tokens that have the least individual hits are dropped first. Set to 0 to disable. Default: 10
	DropTokensThreshold *int `json:"drop_tokens_threshold,omitempty"`

	// If you have some overrides defined but want to disable all of them during query time, you can do that by setting this parameter to false
	EnableOverrides *bool `json:"enable_overrides,omitempty"`

	// List of fields from the document to exclude in the search result
	ExcludeFields *string `json:"exclude_fields,omitempty"`

	// Setting this to true will make Typesense consider all prefixes and typo  corrections of the words in the query without stopping early when enough results are found  (drop_tokens_threshold and typo_tokens_threshold configurations are ignored).
	ExhaustiveSearch *bool `json:"exhaustive_search,omitempty"`

	// A list of fields that will be used for faceting your results on. Separate multiple fields with a comma.
	FacetBy *string `json:"facet_by,omitempty"`

	// Facet values that are returned can now be filtered via this parameter. The matching facet text is also highlighted. For example, when faceting by `category`, you can set `facet_query=category:shoe` to return only facet values that contain the prefix "shoe".
	FacetQuery *string `json:"facet_query,omitempty"`

	// Filter conditions for refining youropen api validator search results. Separate multiple conditions with &&.
	FilterBy *string `json:"filter_by,omitempty"`

	// You can aggregate search results into groups or buckets by specify one or more `group_by` fields. Separate multiple fields with a comma. To group on a particular field, it must be a faceted field.
	GroupBy *string `json:"group_by,omitempty"`

	// Maximum number of hits to be returned for every group. If the `group_limit` is set as `K` then only the top K hits in each group are returned in the response. Default: 3
	GroupLimit *int `json:"group_limit,omitempty"`

	// A list of records to unconditionally hide from search results. A list of `record_id`s to hide. Eg: to hide records with IDs 123 and 456, you'd specify `123,456`.
	// You could also use the Overrides feature to override search results based on rules. Overrides are applied first, followed by `pinned_hits` and finally `hidden_hits`.
	HiddenHits *string `json:"hidden_hits,omitempty"`

	// The number of tokens that should surround the highlighted text on each side. Default: 4
	HighlightAffixNumTokens *int `json:"highlight_affix_num_tokens,omitempty"`

	// The end tag used for the highlighted snippets. Default: `</mark>`
	HighlightEndTag *string `json:"highlight_end_tag,omitempty"`

	// A list of custom fields that must be highlighted even if you don't query  for them
	HighlightFields *string `json:"highlight_fields,omitempty"`

	// List of fields which should be highlighted fully without snippeting
	HighlightFullFields *string `json:"highlight_full_fields,omitempty"`

	// The start tag used for the highlighted snippets. Default: `<mark>`
	HighlightStartTag *string `json:"highlight_start_tag,omitempty"`

	// List of fields from the document to include in the search result
	IncludeFields *string `json:"include_fields,omitempty"`

	// If infix index is enabled for this field, infix searching can be done on a per-field basis by sending a comma separated string parameter called infix to the search query. This parameter can have 3 values; `off` infix search is disabled, which is default `always` infix search is performed along with regular search `fallback` infix search is performed if regular search does not produce results
	Infix *string `json:"infix,omitempty"`

	// There are also 2 parameters that allow you to control the extent of infix searching max_extra_prefix and max_extra_suffix which specify the maximum number of symbols before or after the query that can be present in the token. For example query "K2100" has 2 extra symbols in "6PK2100". By default, any number of prefixes/suffixes can be present for a match.
	MaxExtraPrefix *int `json:"max_extra_prefix,omitempty"`

	// There are also 2 parameters that allow you to control the extent of infix searching max_extra_prefix and max_extra_suffix which specify the maximum number of symbols before or after the query that can be present in the token. For example query "K2100" has 2 extra symbols in "6PK2100". By default, any number of prefixes/suffixes can be present for a match.
	MaxExtraSuffix *int `json:"max_extra_suffix,omitempty"`

	// Maximum number of facet values to be returned.
	MaxFacetValues *int `json:"max_facet_values,omitempty"`

	// Minimum word length for 1-typo correction to be applied.  The value of num_typos is still treated as the maximum allowed typos.
	MinLen1typo *int `json:"min_len_1typo,omitempty"`

	// Minimum word length for 2-typo correction to be applied.  The value of num_typos is still treated as the maximum allowed typos.
	MinLen2typo *int `json:"min_len_2typo,omitempty"`

	// The number of typographical errors (1 or 2) that would be tolerated. Default: 2
	NumTypos *int `json:"num_typos,omitempty"`

	// Results from this specific page number would be fetched.
	Page *int `json:"page,omitempty"`

	// Number of results to fetch per page. Default: 10
	PerPage *int `json:"per_page,omitempty"`

	// A list of records to unconditionally include in the search results at specific positions. An example use case would be to feature or promote certain items on the top of search results. A list of `record_id:hit_position`. Eg: to include a record with ID 123 at Position 1 and another record with ID 456 at Position 5, you'd specify `123:1,456:5`.
	// You could also use the Overrides feature to override search results based on rules. Overrides are applied first, followed by `pinned_hits` and  finally `hidden_hits`.
	PinnedHits *string `json:"pinned_hits,omitempty"`

	// You can index content from any logographic language into Typesense if you are able to segment / split the text into space-separated words yourself  before indexing and querying.
	// Set this parameter to true to do the same
	PreSegmentedQuery *bool `json:"pre_segmented_query,omitempty"`

	// Boolean field to indicate that the last word in the query should be treated as a prefix, and not as a whole word. This is used for building autocomplete and instant search interfaces. Defaults to true.
	Prefix *string `json:"prefix,omitempty"`

	// Set this parameter to true to ensure that an exact match is ranked above the others
	PrioritizeExactMatch *bool `json:"prioritize_exact_match,omitempty"`

	// The query text to search for in the collection. Use * as the search string to return all documents. This is typically useful when used in conjunction with filter_by.
	Q *string `json:"q,omitempty"`

	// A list of `string` fields that should be queried against. Multiple fields are separated with a comma.
	QueryBy *string `json:"query_by,omitempty"`

	// The relative weight to give each `query_by` field when ranking results. This can be used to boost fields in priority, when looking for matches. Multiple fields are separated with a comma.
	QueryByWeights *string `json:"query_by_weights,omitempty"`

	// Typesense will attempt to return results early if the cutoff time has elapsed.  This is not a strict guarantee and facet computation is not bound by this parameter.
	SearchCutoffMs *int `json:"search_cutoff_ms,omitempty"`

	// Field values under this length will be fully highlighted, instead of showing a snippet of relevant portion. Default: 30
	SnippetThreshold *int `json:"snippet_threshold,omitempty"`

	// A list of numerical fields and their corresponding sort orders that will be used for ordering your results. Up to 3 sort fields can be specified. The text similarity score is exposed as a special `_text_match` field that you can use in the list of sorting fields. If no `sort_by` parameter is specified, results are sorted by `_text_match:desc,default_sorting_field:desc`
	SortBy *string `json:"sort_by,omitempty"`

	// If the number of results found for a specific query is less than this number, Typesense will attempt to look for tokens with more typos until enough results are found. Default: 100
	TypoTokensThreshold *int `json:"typo_tokens_threshold,omitempty"`

	// Enable server side caching of search query results. By default, caching is disabled.
	UseCache *bool `json:"use_cache,omitempty"`
}
