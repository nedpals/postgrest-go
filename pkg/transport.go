package postgrest_go

import (
	"net/http"
	"net/url"
)

type PostgrestTransport struct {
	params  url.Values
	header  http.Header
	baseURL url.URL

	parent http.RoundTripper
}

func (c PostgrestTransport) AddHeader(key string, value string) {
	c.header.Add(key, value)
}

func (c PostgrestTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header = c.header
	req.URL = c.baseURL.ResolveReference(req.URL)
	req.URL.RawQuery = c.params.Encode()
	return c.parent.RoundTrip(req)
}
