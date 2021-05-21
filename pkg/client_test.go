package postgrest_go

import (
	"net/url"
	"testing"
)

func TestPostgrestClient_Constructor(t *testing.T) {
	client := NewClient(url.URL{Scheme: "https", Host: "example.com"})

	if got := client.Transport.baseURL.String(); got != "https://example.com" {
		t.Errorf("expected baseURL == %s, got %s", "https://example.com", got)
	}

	if got := client.Transport.header.Get("Accept"); got != "application/json" {
		t.Errorf("expected header Accept == %s, got %s", "application/json", got)
	}
	if got := client.Transport.header.Get("Content-Type"); got != "application/json" {
		t.Errorf("expected header Content-Type == %s, got %s", "application/json", got)
	}
	if got := client.Transport.header.Get("Accept-Profile"); got != "public" {
		t.Errorf("expected header Accept-Profile == %s, got %s", "public", got)
	}
	if got := client.Transport.header.Get("Content-Profile"); got != "public" {
		t.Errorf("expected header Content-Profile == %s, got %s", "public", got)
	}
}

func TestPostgrestClient_TokenAuth(t *testing.T) {
	client := NewClient(
		url.URL{Scheme: "https", Host: "example.com"},
		WithTokenAuth("s3cr3t"))

	if got := client.Transport.header.Get("Authorization"); got != "Bearer s3cr3t" {
		t.Errorf("expected header Authorization == %s, got %s", "Bearer s3cr3t", got)
	}
}

func TestPostgrestClient_BasicAuth(t *testing.T) {
	client := NewClient(
		url.URL{Scheme: "https", Host: "example.com"},
		WithBasicAuth("admin", "s3cr3t"))

	if got := client.Transport.header.Get("Authorization"); got != "Basic YWRtaW46czNjcjN0" {
		t.Errorf("expected header Authorization == %s, got %s", "Basic YWRtaW46czNjcjN0", got)
	}
}

func TestPostgrestClient_Schema(t *testing.T) {
	client := NewClient(
		url.URL{Scheme: "https", Host: "example.com"},
		WithSchema("private"))

	if got := client.Transport.header.Get("Accept-Profile"); got != "private" {
		t.Errorf("expected header Accept-Profile == %s, got %s", "private", got)
	}
	if got := client.Transport.header.Get("Content-Profile"); got != "private" {
		t.Errorf("expected header Content-Profile == %s, got %s", "private", got)
	}
}
