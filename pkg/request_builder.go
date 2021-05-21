package postgrest_go

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type RequestBuilder struct {
	client Client
	path   string
}

func (b RequestBuilder) Select(columns ...string) SelectRequestBuilder {
	b.client.Transport.params.Set("select", strings.Join(columns, ","))
	return SelectRequestBuilder{
		FilterRequestBuilder{
			QueryRequestBuilder: QueryRequestBuilder{client: b.client, path: b.path, httpMethod: "GET"},
			negateNext:          false,
		},
	}
}

func (b RequestBuilder) Insert(json interface{}) QueryRequestBuilder {
	b.client.Transport.header.Add("Prefer", "return=representation")
	return QueryRequestBuilder{client: b.client, path: b.path, httpMethod: "POST", json: json}
}

func (b RequestBuilder) Upsert(json interface{}) QueryRequestBuilder {
	b.client.Transport.header.Add("Prefer", "return=representation,resolution=merge-duplicates")
	return QueryRequestBuilder{client: b.client, path: b.path, httpMethod: "POST", json: json}
}

func (b RequestBuilder) Update(json interface{}) FilterRequestBuilder {
	b.client.Transport.header.Add("Prefer", "return=representation")
	return FilterRequestBuilder{
		QueryRequestBuilder: QueryRequestBuilder{client: b.client, path: b.path, httpMethod: "PATCH", json: json},
		negateNext:          false,
	}
}

func (b RequestBuilder) Delete() FilterRequestBuilder {
	return FilterRequestBuilder{
		QueryRequestBuilder: QueryRequestBuilder{client: b.client, path: b.path, httpMethod: "DELETE", json: nil},
		negateNext:          false,
	}
}

type QueryRequestBuilder struct {
	client     Client
	path       string
	httpMethod string
	json       interface{}
}

func (b QueryRequestBuilder) Execute(r interface{}) error {
	return b.ExecuteWithContext(context.Background(), r)
}

func (b QueryRequestBuilder) ExecuteWithContext(ctx context.Context, r interface{}) error {
	data, err := json.Marshal(b.json)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, b.httpMethod, b.path, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	resp, err := b.client.session.Do(req)
	if err != nil {
		return err
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(body, r); err != nil {
		return err
	}
	return nil
}

type FilterRequestBuilder struct {
	QueryRequestBuilder
	negateNext bool
}

func (b FilterRequestBuilder) Not() FilterRequestBuilder {
	b.negateNext = true
	return b
}

func (b FilterRequestBuilder) Filter(column, operator, criteria string) FilterRequestBuilder {
	if b.negateNext {
		b.negateNext = false
		operator = "not." + operator
	}
	b.client.Transport.params.Add(SanitizeParam(column), operator+"."+criteria)
	return b
}

func (b FilterRequestBuilder) Eq(column, value string) FilterRequestBuilder {
	return b.Filter(column, "eq", SanitizeParam(value))
}

func (b FilterRequestBuilder) Neq(column, value string) FilterRequestBuilder {
	return b.Filter(column, "neq", SanitizeParam(value))
}

func (b FilterRequestBuilder) Gt(column, value string) FilterRequestBuilder {
	return b.Filter(column, "gt", SanitizeParam(value))
}

func (b FilterRequestBuilder) Gte(column, value string) FilterRequestBuilder {
	return b.Filter(column, "gte", SanitizeParam(value))
}

func (b FilterRequestBuilder) Lt(column, value string) FilterRequestBuilder {
	return b.Filter(column, "lt", SanitizeParam(value))
}

func (b FilterRequestBuilder) Lte(column, value string) FilterRequestBuilder {
	return b.Filter(column, "lte", SanitizeParam(value))
}

func (b FilterRequestBuilder) Is(column, value string) FilterRequestBuilder {
	return b.Filter(column, "is", SanitizeParam(value))
}

func (b FilterRequestBuilder) Like(column, value string) FilterRequestBuilder {
	return b.Filter(column, "like", SanitizeParam(value))
}

func (b FilterRequestBuilder) Ilike(column, value string) FilterRequestBuilder {
	return b.Filter(column, "ilike", SanitizeParam(value))
}

func (b FilterRequestBuilder) Fts(column, value string) FilterRequestBuilder {
	return b.Filter(column, "fts", SanitizeParam(value))
}

func (b FilterRequestBuilder) Plfts(column, value string) FilterRequestBuilder {
	return b.Filter(column, "plfts", SanitizeParam(value))
}

func (b FilterRequestBuilder) Phfts(column, value string) FilterRequestBuilder {
	return b.Filter(column, "phfts", SanitizeParam(value))
}

func (b FilterRequestBuilder) Wfts(column, value string) FilterRequestBuilder {
	return b.Filter(column, "wfts", SanitizeParam(value))
}

func (b FilterRequestBuilder) In(column string, values []string) FilterRequestBuilder {
	sanitized := make([]string, len(values))
	for i, value := range values {
		sanitized[i] = SanitizeParam(value)
	}
	return b.Filter(column, "in", fmt.Sprintf("(%s)", strings.Join(sanitized, ",")))
}

func (b FilterRequestBuilder) Cs(column string, values []string) FilterRequestBuilder {
	sanitized := make([]string, len(values))
	for i, value := range values {
		sanitized[i] = SanitizeParam(value)
	}
	return b.Filter(column, "cs", fmt.Sprintf("{%s}", strings.Join(sanitized, ",")))
}

func (b FilterRequestBuilder) Cd(column string, values []string) FilterRequestBuilder {
	sanitized := make([]string, len(values))
	for i, value := range values {
		sanitized[i] = SanitizeParam(value)
	}
	return b.Filter(column, "cd", fmt.Sprintf("{%s}", strings.Join(sanitized, ",")))
}

func (b FilterRequestBuilder) Ov(column string, values []string) FilterRequestBuilder {
	sanitized := make([]string, len(values))
	for i, value := range values {
		sanitized[i] = SanitizeParam(value)
	}
	return b.Filter(column, "ov", fmt.Sprintf("{%s}", strings.Join(sanitized, ",")))
}

func (b FilterRequestBuilder) Sl(column string, from, to int) FilterRequestBuilder {
	return b.Filter(column, "sl", fmt.Sprintf("(%d,%d)", from, to))
}

func (b FilterRequestBuilder) Sr(column string, from, to int) FilterRequestBuilder {
	return b.Filter(column, "sr", fmt.Sprintf("(%d,%d)", from, to))
}

func (b FilterRequestBuilder) Nxl(column string, from, to int) FilterRequestBuilder {
	return b.Filter(column, "nxl", fmt.Sprintf("(%d,%d)", from, to))
}

func (b FilterRequestBuilder) Nxr(column string, from, to int) FilterRequestBuilder {
	return b.Filter(column, "nxr", fmt.Sprintf("(%d,%d)", from, to))
}

func (b FilterRequestBuilder) Adj(column string, from, to int) FilterRequestBuilder {
	return b.Filter(column, "adj", fmt.Sprintf("(%d,%d)", from, to))
}

type SelectRequestBuilder struct {
	FilterRequestBuilder
}

func (b SelectRequestBuilder) Limit(size int) SelectRequestBuilder {
	return b.LimitWithOffset(size, 0)
}

func (b SelectRequestBuilder) LimitWithOffset(size int, start int) SelectRequestBuilder {
	b.client.Transport.AddHeader("Range-Unit", "items")
	b.client.Transport.AddHeader("Range", fmt.Sprintf("%d-%d", start, start+size-1))
	return b
}

func (b SelectRequestBuilder) Single() SelectRequestBuilder {
	b.client.Transport.AddHeader("Accept", "application/vnd.pgrst.object+json")
	return b
}
