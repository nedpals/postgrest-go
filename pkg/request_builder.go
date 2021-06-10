package postgrest_go

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// TODO test
type RequestError struct {
	Message        string `json:"message"`
	Details        string `json:"details"`
	Hint           string `json:"hint"`
	Code           string `json:"code"`
	HTTPStatusCode int    `json:"-"`
}

func (rq *RequestError) Error() string {
	return fmt.Sprintf("%s: %s", rq.Code, rq.Message)
}

type RequestBuilder struct {
	client *Client
	path   string
	params url.Values
	header http.Header
}

func (b *RequestBuilder) Select(columns ...string) *SelectRequestBuilder {
	b.params.Set("select", strings.Join(columns, ","))
	return &SelectRequestBuilder{
		FilterRequestBuilder{
			QueryRequestBuilder: QueryRequestBuilder{
				client:     b.client,
				path:       b.path,
				httpMethod: "GET",
				header:     b.header,
				params:     b.params,
			},
			negateNext: false,
		},
	}
}

func (b *RequestBuilder) Insert(json interface{}) *QueryRequestBuilder {
	b.header.Set("Prefer", "return=representation")
	return &QueryRequestBuilder{
		client:     b.client,
		path:       b.path,
		httpMethod: http.MethodPost,
		json:       json,
		params:     b.params,
		header:     b.header,
	}
}

func (b *RequestBuilder) Upsert(json interface{}) *QueryRequestBuilder {
	b.header.Set("Prefer", "return=representation,resolution=merge-duplicates")
	return &QueryRequestBuilder{
		client:     b.client,
		path:       b.path,
		httpMethod: http.MethodPost,
		json:       json,
		params:     b.params,
		header:     b.header,
	}
}

func (b *RequestBuilder) Update(json interface{}) *FilterRequestBuilder {
	b.header.Set("Prefer", "return=representation")
	return &FilterRequestBuilder{
		QueryRequestBuilder: QueryRequestBuilder{
			client:     b.client,
			path:       b.path,
			httpMethod: http.MethodPatch,
			json:       json,
			params:     b.params,
			header:     b.header,
		},
		negateNext: false,
	}
}

func (b *RequestBuilder) Delete() *FilterRequestBuilder {
	return &FilterRequestBuilder{
		QueryRequestBuilder: QueryRequestBuilder{
			client:     b.client,
			path:       b.path,
			httpMethod: http.MethodDelete,
			json:       nil,
			params:     b.params,
			header:     b.header,
		},
		negateNext: false,
	}
}

type QueryRequestBuilder struct {
	client     *Client
	params     url.Values
	header     http.Header
	path       string
	httpMethod string
	json       interface{}
}

func (b *QueryRequestBuilder) Execute(r interface{}) error {
	return b.ExecuteWithContext(context.Background(), r)
}

func (b *QueryRequestBuilder) ExecuteWithContext(ctx context.Context, r interface{}) error {
	data, err := json.Marshal(b.json)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, b.httpMethod, b.path, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	req.URL.RawQuery = b.params.Encode()
	req.Header = b.client.Headers()

	// inject/override custom headers
	for key, vals := range b.header {
		for _, val := range vals {
			req.Header.Set(key, val)
		}
	}

	req.URL.Path = req.URL.Path[1:]
	req.URL = b.client.Transport.baseURL.ResolveReference(req.URL)

	resp, err := b.client.session.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	statusOK := resp.StatusCode >= 200 && resp.StatusCode < 300
	if !statusOK {
		reqError := RequestError{HTTPStatusCode: resp.StatusCode}

		if err = json.Unmarshal(body, &reqError); err != nil {
			return err
		}

		return &reqError
	}

	if resp.StatusCode != http.StatusNoContent && r != nil {
		if err = json.Unmarshal(body, r); err != nil {
			return err
		}
	}

	return nil
}

type FilterRequestBuilder struct {
	QueryRequestBuilder
	negateNext bool
}

func (b *FilterRequestBuilder) Not() *FilterRequestBuilder {
	b.negateNext = true
	return b
}

func (b *FilterRequestBuilder) Filter(column, operator, criteria string) *FilterRequestBuilder {
	if b.negateNext {
		b.negateNext = false
		operator = "not." + operator
	}
	b.params.Add(SanitizeParam(column), operator+"."+criteria)
	return b
}

func (b *FilterRequestBuilder) Eq(column, value string) *FilterRequestBuilder {
	return b.Filter(column, "eq", SanitizeParam(value))
}

func (b *FilterRequestBuilder) Neq(column, value string) *FilterRequestBuilder {
	return b.Filter(column, "neq", SanitizeParam(value))
}

func (b *FilterRequestBuilder) Gt(column, value string) *FilterRequestBuilder {
	return b.Filter(column, "gt", SanitizeParam(value))
}

func (b *FilterRequestBuilder) Gte(column, value string) *FilterRequestBuilder {
	return b.Filter(column, "gte", SanitizeParam(value))
}

func (b *FilterRequestBuilder) Lt(column, value string) *FilterRequestBuilder {
	return b.Filter(column, "lt", SanitizeParam(value))
}

func (b *FilterRequestBuilder) Lte(column, value string) *FilterRequestBuilder {
	return b.Filter(column, "lte", SanitizeParam(value))
}

func (b *FilterRequestBuilder) Is(column, value string) *FilterRequestBuilder {
	return b.Filter(column, "is", SanitizeParam(value))
}

func (b *FilterRequestBuilder) Like(column, value string) *FilterRequestBuilder {
	return b.Filter(column, "like", SanitizeParam(value))
}

func (b *FilterRequestBuilder) Ilike(column, value string) *FilterRequestBuilder {
	return b.Filter(column, "ilike", SanitizeParam(value))
}

func (b *FilterRequestBuilder) Fts(column, value string) *FilterRequestBuilder {
	return b.Filter(column, "fts", SanitizeParam(value))
}

func (b *FilterRequestBuilder) Plfts(column, value string) *FilterRequestBuilder {
	return b.Filter(column, "plfts", SanitizeParam(value))
}

func (b *FilterRequestBuilder) Phfts(column, value string) *FilterRequestBuilder {
	return b.Filter(column, "phfts", SanitizeParam(value))
}

func (b *FilterRequestBuilder) Wfts(column, value string) *FilterRequestBuilder {
	return b.Filter(column, "wfts", SanitizeParam(value))
}

func (b *FilterRequestBuilder) In(column string, values []string) *FilterRequestBuilder {
	sanitized := make([]string, len(values))
	for i, value := range values {
		sanitized[i] = SanitizeParam(value)
	}
	return b.Filter(column, "in", fmt.Sprintf("(%s)", strings.Join(sanitized, ",")))
}

func (b *FilterRequestBuilder) Cs(column string, values []string) *FilterRequestBuilder {
	sanitized := make([]string, len(values))
	for i, value := range values {
		sanitized[i] = SanitizeParam(value)
	}
	return b.Filter(column, "cs", fmt.Sprintf("{%s}", strings.Join(sanitized, ",")))
}

func (b *FilterRequestBuilder) Cd(column string, values []string) *FilterRequestBuilder {
	sanitized := make([]string, len(values))
	for i, value := range values {
		sanitized[i] = SanitizeParam(value)
	}
	return b.Filter(column, "cd", fmt.Sprintf("{%s}", strings.Join(sanitized, ",")))
}

func (b *FilterRequestBuilder) Ov(column string, values []string) *FilterRequestBuilder {
	sanitized := make([]string, len(values))
	for i, value := range values {
		sanitized[i] = SanitizeParam(value)
	}
	return b.Filter(column, "ov", fmt.Sprintf("{%s}", strings.Join(sanitized, ",")))
}

func (b *FilterRequestBuilder) Sl(column string, from, to int) *FilterRequestBuilder {
	return b.Filter(column, "sl", fmt.Sprintf("(%d,%d)", from, to))
}

func (b *FilterRequestBuilder) Sr(column string, from, to int) *FilterRequestBuilder {
	return b.Filter(column, "sr", fmt.Sprintf("(%d,%d)", from, to))
}

func (b *FilterRequestBuilder) Nxl(column string, from, to int) *FilterRequestBuilder {
	return b.Filter(column, "nxl", fmt.Sprintf("(%d,%d)", from, to))
}

func (b *FilterRequestBuilder) Nxr(column string, from, to int) *FilterRequestBuilder {
	return b.Filter(column, "nxr", fmt.Sprintf("(%d,%d)", from, to))
}

func (b *FilterRequestBuilder) Adj(column string, from, to int) *FilterRequestBuilder {
	return b.Filter(column, "adj", fmt.Sprintf("(%d,%d)", from, to))
}

type SelectRequestBuilder struct {
	FilterRequestBuilder
}

func (b *SelectRequestBuilder) Limit(size int) *SelectRequestBuilder {
	return b.LimitWithOffset(size, 0)
}

func (b *SelectRequestBuilder) LimitWithOffset(size int, start int) *SelectRequestBuilder {
	b.header.Set("Range-Unit", "items")
	b.header.Set("Range", fmt.Sprintf("%d-%d", start, start+size-1))
	return b
}

func (b *SelectRequestBuilder) Single() *SelectRequestBuilder {
	b.header.Set("Accept", "application/vnd.pgrst.object+json")
	return b
}
