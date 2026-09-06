package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestTokenBucketRateLimiting(t *testing.T) {
	// Rate of 10 tokens/sec, capacity of 2 tokens
	tb := NewTokenBucket(10, 2)

	// 1st request should pass
	if !tb.Allow() {
		t.Fatalf("expected 1st token to be allowed")
	}

	// 2nd request should pass (burst capacity exhausted)
	if !tb.Allow() {
		t.Fatalf("expected 2nd token to be allowed")
	}

	// 3rd immediate request should be rejected
	if tb.Allow() {
		t.Fatalf("expected 3rd token to be rejected when burst is exhausted")
	}

	// Wait 150ms for tokens to refill (10 tokens/sec = 1 token per 100ms)
	time.Sleep(150 * time.Millisecond)

	// Should allow after refill
	if !tb.Allow() {
		t.Fatalf("expected token to be allowed after refill period")
	}
}

func TestRateLimitMiddlewareHTTP(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()

	// Rate limit: 2 tokens burst capacity, 5 tokens/sec
	router.POST("/test", RateLimitMiddleware(5, 2), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// 1st request -> 200 OK
	w1 := httptest.NewRecorder()
	req1, _ := http.NewRequest("POST", "/test", nil)
	req1.RemoteAddr = "192.168.1.100:12345"
	router.ServeHTTP(w1, req1)
	if w1.Code != http.StatusOK {
		t.Fatalf("expected status 200 on 1st request, got %d", w1.Code)
	}

	// 2nd request -> 200 OK
	w2 := httptest.NewRecorder()
	req2, _ := http.NewRequest("POST", "/test", nil)
	req2.RemoteAddr = "192.168.1.100:12345"
	router.ServeHTTP(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("expected status 200 on 2nd request, got %d", w2.Code)
	}

	// 3rd immediate request -> 429 Too Many Requests
	w3 := httptest.NewRecorder()
	req3, _ := http.NewRequest("POST", "/test", nil)
	req3.RemoteAddr = "192.168.1.100:12345"
	router.ServeHTTP(w3, req3)
	if w3.Code != http.StatusTooManyRequests {
		t.Fatalf("expected status 429 on 3rd request, got %d", w3.Code)
	}

	// Header Retry-After should be present
	if w3.Header().Get("Retry-After") != "1" {
		t.Fatalf("expected Retry-After header '1', got '%s'", w3.Header().Get("Retry-After"))
	}
}

func TestSlidingWindowCounter(t *testing.T) {
	// Limit: 3 requests per 100ms window
	sw := NewSlidingWindowCounter(3, 100*time.Millisecond)

	if !sw.Allow() {
		t.Fatalf("expected 1st request to pass")
	}
	if !sw.Allow() {
		t.Fatalf("expected 2nd request to pass")
	}
	if !sw.Allow() {
		t.Fatalf("expected 3rd request to pass")
	}

	// 4th request in same window should be blocked
	if sw.Allow() {
		t.Fatalf("expected 4th request to be blocked")
	}

	// Wait for window rotation and partial decay (120ms)
	time.Sleep(120 * time.Millisecond)

	// Should allow requests in the new window
	if !sw.Allow() {
		t.Fatalf("expected request in next window to be allowed")
	}
}

