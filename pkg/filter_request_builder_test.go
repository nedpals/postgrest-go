package postgrest_go

import (
	"net/url"
	"testing"
)

func TestFilterRequestBuilder_Constructor(t *testing.T) {
	client := NewClient(url.URL{Scheme: "https", Host: "example.com"})

	path := "/example_table"
	httpMethod := "GET"

	builder := FilterRequestBuilder{
		QueryRequestBuilder: QueryRequestBuilder{
			client:     client,
			path:       path,
			httpMethod: httpMethod,
			json:       nil,
		},
		negateNext: false,
	}

	if builder.path != path {
		t.Errorf("expected path == %s, got %s", path, builder.path)
	}
	if builder.httpMethod != httpMethod {
		t.Errorf("expected httpMethod == %s, got %s", httpMethod, builder.httpMethod)
	}
	if builder.json != nil {
		t.Errorf("expected json == %v, got %v", nil, builder.json)
	}
}

func TestFilterRequestBuilder_Not(t *testing.T) {
	client := NewClient(url.URL{Scheme: "https", Host: "example.com"})

	path := "/example_table"
	httpMethod := "GET"

	builder := FilterRequestBuilder{
		QueryRequestBuilder: QueryRequestBuilder{
			client:     client,
			path:       path,
			httpMethod: httpMethod,
			json:       nil,
		},
		negateNext: false,
	}

	if got := builder.Not().negateNext; !got {
		t.Errorf("expected negateNext == true, got %v", got)
	}
}

func TestFilterRequestBuilder_Filter(t *testing.T) {
	client := NewClient(url.URL{Scheme: "https", Host: "example.com"})

	path := "/example_table"
	httpMethod := "GET"

	builder := FilterRequestBuilder{
		QueryRequestBuilder: QueryRequestBuilder{
			client:     client,
			path:       path,
			httpMethod: httpMethod,
			json:       nil,
		},
		negateNext: false,
	}

	builder = builder.Filter(":col.name", "eq", "val")

	want := "eq.val"
	got := builder.client.transport.params.Get("\":col.name\"")

	if want != got {
		t.Errorf("expected http param \":col.name\" == %s, got %s", want, got)
	}
}

func TestFilterRequestBuilder_MultivaluedParam(t *testing.T) {
	client := NewClient(url.URL{Scheme: "https", Host: "example.com"})

	path := "/example_table"
	httpMethod := "GET"

	builder := FilterRequestBuilder{
		QueryRequestBuilder: QueryRequestBuilder{
			client:     client,
			path:       path,
			httpMethod: httpMethod,
			json:       nil,
		},
		negateNext: false,
	}

	builder = builder.Lte("x", "a").Gte("x", "b")

	want := "x=lte.a&x=gte.b"
	got := builder.client.transport.params.Encode()

	if want != got {
		t.Errorf("expected http params.Encode() == %s, got %s", want, got)
	}
}
