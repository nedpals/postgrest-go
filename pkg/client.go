package postgrest_go

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
)

type Client struct {
	session   http.Client
	Transport PostgrestTransport
}

type ClientOption func(c Client)

func NewClient(baseURL url.URL, opts ...ClientOption) Client {
	transport := PostgrestTransport{
		params:  url.Values{},
		header:  http.Header{},
		baseURL: baseURL,
	}
	c := Client{
		Transport: transport,
		session:   http.Client{Transport: transport},
	}

	c.Transport.AddHeader("Accept", "application/json")
	c.Transport.AddHeader("Content-Type", "application/json")
	c.Transport.AddHeader("Accept-Profile", "public")
	c.Transport.AddHeader("Content-Profile", "public")

	for _, opt := range opts {
		opt(c)
	}

	return c
}

func (c Client) From(table string) RequestBuilder {
	return RequestBuilder{client: c, path: "/" + table}
}

func (c Client) Rpc(f string, params interface{}) (*http.Response, error) {
	b, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", "/rpc/"+f, bytes.NewBuffer(b))
	if err != nil {
		return nil, err
	}
	resp, err := c.session.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c Client) CloseIdleConnections() {
	c.session.CloseIdleConnections()
}

func WithTokenAuth(token string) ClientOption {
	return func(c Client) {
		c.Transport.AddHeader("Authorization", "Bearer "+token)
	}
}

func WithBasicAuth(username, password string) ClientOption {
	return func(c Client) {
		c.Transport.AddHeader("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(username+":"+password)))
	}
}

func WithSchema(schema string) ClientOption {
	return func(c Client) {
		c.Transport.AddHeader("Accept-Profile", schema)
		c.Transport.AddHeader("Content-Profile", schema)
	}
}
