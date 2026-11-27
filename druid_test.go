package druid

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSetBaseURLWithSuffix(t *testing.T) {
	assert := assert.New(t)

	d, err := NewClient("localhost:8082")
	assert.Nil(err, "error should be nil")
	assert.NotNil(d, "client should not be nil")

	wantBaseURL, _ := url.ParseRequestURI("/")
	err = d.setBaseURL("")
	assert.Nil(err, "error should be nil")
	assert.Equal(d.baseURL, wantBaseURL, "they should not be equal")
}

func TestNewClientWithSkipVerify(t *testing.T) {
	assert := assert.New(t)

	var druidOpts []ClientOption
	druidOpts = append(druidOpts, WithSkipTLSVerify())

	d, err := NewClient("localhost:8082", druidOpts...)
	assert.Nil(err, "error should be nil")
	assert.NotNil(d, "client should not be nil")
	assert.True(d.http.HTTPClient.Transport.(*http.Transport).TLSClientConfig.InsecureSkipVerify, "insecure skip verify should be true")
}

// TODO: at some point use https://golang.org/src/crypto/tls/example_test.go this to create server with bad cert and test

func TestDefaultRetry(t *testing.T) {
	ctx := context.TODO()
	var b string
	var expectedErr error

	resp := buildMockResp(200, b)
	retry, err := defaultRetry(ctx, &resp, nil)
	assert.Nil(t, err)
	assert.False(t, retry)

	b = `{
		"error": "SQL parse failed", "errorMessage" : "Something bad happened."
	}`
	expectedErr = fmt.Errorf("failed to query Druid: {Error:SQL parse failed ErrorMessage:Something bad happened. ErrorClass: Host:}")
	resp = buildMockResp(400, b)
	retry, err = defaultRetry(ctx, &resp, nil)
	assert.NotNil(t, err)
	assert.Equal(t, expectedErr, err)
	assert.False(t, retry)

	b = `{
		"error": "Plan validation failed", "errorMessage" : "Something bad happened."
	}`
	expectedErr = fmt.Errorf("failed to query Druid: {Error:Plan validation failed ErrorMessage:Something bad happened. ErrorClass: Host:}")
	resp = buildMockResp(400, b)
	retry, err = defaultRetry(ctx, &resp, nil)
	assert.NotNil(t, err)
	assert.Equal(t, expectedErr, err)
	assert.False(t, retry)

	b = `{
		"error": "Resource limit exceeded", "errorMessage" : "Something bad happened."
	}`
	expectedErr = fmt.Errorf("error response from Druid: {Error:Resource limit exceeded ErrorMessage:Something bad happened. ErrorClass: Host:}")
	resp = buildMockResp(400, b)
	retry, err = defaultRetry(ctx, &resp, nil)
	assert.NotNil(t, err)
	assert.Equal(t, expectedErr, err)
	assert.True(t, retry)

	b = `{
		"error": "Query capacity exceeded", "errorMessage" : "Something bad happened."
	}`
	expectedErr = fmt.Errorf("error response from Druid: {Error:Query capacity exceeded ErrorMessage:Something bad happened. ErrorClass: Host:}")
	resp = buildMockResp(429, b)
	retry, err = defaultRetry(ctx, &resp, nil)
	assert.NotNil(t, err)
	assert.Equal(t, expectedErr, err)
	assert.True(t, retry)

	b = `{
		"error": "Unsupported operation", "errorMessage" : "Something bad happened."
	}`
	expectedErr = fmt.Errorf("failed to query Druid: {Error:Unsupported operation ErrorMessage:Something bad happened. ErrorClass: Host:}")
	resp = buildMockResp(501, b)
	retry, err = defaultRetry(ctx, &resp, nil)
	assert.NotNil(t, err)
	assert.Equal(t, expectedErr, err)
	assert.False(t, retry)

	b = `{
		"error": "Query timeout", "errorMessage" : "Something bad happened."
	}`
	resp = buildMockResp(504, b)
	retry, err = defaultRetry(ctx, &resp, nil)
	assert.EqualError(t, err, "gateway timeout: "+strings.TrimSpace(b))
	assert.False(t, retry)

	b = `{
		"error": "Query cancelled", "errorMessage" : "Something bad happened."
	}`
	expectedErr = fmt.Errorf("failed to query Druid: {Error:Query cancelled ErrorMessage:Something bad happened. ErrorClass: Host:}")
	resp = buildMockResp(500, b)
	retry, err = defaultRetry(ctx, &resp, nil)
	assert.NotNil(t, err)
	assert.Equal(t, expectedErr, err)
	assert.False(t, retry)

	b = `{
		"error": "Unknown exception", "errorMessage" : "Something bad happened."
	}`
	expectedErr = fmt.Errorf("failed to query Druid: {Error:Unknown exception ErrorMessage:Something bad happened. ErrorClass: Host:}")
	resp = buildMockResp(500, b)
	retry, err = defaultRetry(ctx, &resp, nil)
	assert.NotNil(t, err)
	assert.Equal(t, expectedErr, err)
	assert.False(t, retry)

	b = `invalid json`
	expectedErr = fmt.Errorf("failed to read the response from Druid: invalid character 'i' looking for beginning of value")
	resp = buildMockResp(500, b)
	retry, err = defaultRetry(ctx, &resp, nil)
	assert.NotNil(t, err)
	assert.Equal(t, expectedErr.Error(), err.Error())
	assert.True(t, retry)
}

func TestDefaultRetryGatewayTimeoutEmptyBody(t *testing.T) {
	resp := buildMockResp(504, "")
	retry, err := defaultRetry(context.TODO(), &resp, nil)
	assert.EqualError(t, err, "gateway timeout")
	assert.False(t, retry)
}

func TestDefaultRetryGatewayTimeoutBodyIsLimited(t *testing.T) {
	resp := buildMockResp(504, strings.Repeat("a", 5000))
	retry, err := defaultRetry(context.TODO(), &resp, nil)
	assert.Len(t, err.Error(), len("gateway timeout: ")+4096)
	assert.False(t, retry)
}

func TestQueryNotRetriedOnGatewayTimeout(t *testing.T) {
	var count int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusGatewayTimeout)
		fmt.Fprint(w, `{"error":"Query timeout"}`)
	}))
	defer srv.Close()

	c, err := NewClient(srv.URL, WithRetryWaitMin(time.Millisecond), WithRetryWaitMax(time.Millisecond))
	assert.Nil(t, err)
	_, err = c.ExecuteRequest("POST", "druid/v2", map[string]string{}, nil)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "gateway timeout")
	assert.Contains(t, err.Error(), "Query timeout")
	assert.Equal(t, int32(1), atomic.LoadInt32(&count))
}

func TestQueryRetriedOnServiceUnavailable(t *testing.T) {
	var count int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, `{"error":"Query capacity exceeded"}`)
	}))
	defer srv.Close()

	c, err := NewClient(srv.URL, WithRetryWaitMin(time.Millisecond), WithRetryWaitMax(time.Millisecond))
	assert.Nil(t, err)
	_, err = c.ExecuteRequest("POST", "druid/v2", map[string]string{}, nil)
	assert.NotNil(t, err)
	assert.Equal(t, int32(6), atomic.LoadInt32(&count))
}

func buildMockResp(statusCode int, body string) http.Response {
	var st string
	switch statusCode {
	case 200:
		st = "200 OK"
	case 400:
		st = "400 Bad Request"
	case 429:
		st = "429 Too Many Requests"
	case 500:
		st = "500 Internal Server Error"
	case 501:
		st = "Not Implemented"
	case 504:
		st = "Gateway Timeout"
	default:
		panic(fmt.Errorf("Unsupported mock status code: %d", statusCode))
	}
	return http.Response{
		Status: st, StatusCode: statusCode,
		Body: ioutil.NopCloser(strings.NewReader(body)),
	}
}
