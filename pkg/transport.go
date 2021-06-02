package postgrest_go

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type PostgrestTransport struct {
	params  url.Values
	header  http.Header
	baseURL url.URL
	debug   bool

	parent http.RoundTripper
}

func (c PostgrestTransport) AddHeader(key string, value string) {
	c.header.Set(key, value)
}

func (c PostgrestTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	for key, vals := range c.header {
		for _, val := range vals {
			req.Header.Set(key, val)
		}
	}

	req.URL.Path = req.URL.Path[1:]
	req.URL = c.baseURL.ResolveReference(req.URL)
	req.URL.RawQuery = c.params.Encode()

	if c.debug {
		fmt.Println("--- incoming postgrest-go req ---")
		fmt.Printf("%s %s\n", req.Method, req.URL.String())
		for key, headerValues := range req.Header {
			for _, val := range headerValues {
				fmt.Printf("%s: %s\n", key, val)
			}
		}

		if body, err := io.ReadAll(req.Body); err == nil {
			fmt.Println(string(body))
		} else {
			fmt.Println("Request body not shown because of an error")
		}
		fmt.Println("---------------------------------")
	}
	return c.parent.RoundTrip(req)
}
