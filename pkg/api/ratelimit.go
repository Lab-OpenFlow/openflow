package api

import (
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// TokenBucket implements a thread-safe token bucket algorithm.
type TokenBucket struct {
	mu         sync.Mutex
	rate       float64   // tokens per second
	capacity   float64   // maximum burst capacity
	tokens     float64   // current available tokens
	lastRefill time.Time // timestamp of last token refill
}

// NewTokenBucket creates a new token bucket.
func NewTokenBucket(rate, capacity float64) *TokenBucket {
	return &TokenBucket{
		rate:       rate,
		capacity:   capacity,
		tokens:     capacity,
		lastRefill: time.Now(),
	}
}

// Allow checks if a token is available and consumes it.
func (tb *TokenBucket) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.lastRefill = now

	// Refill tokens according to elapsed time
	tb.tokens = math.Min(tb.capacity, tb.tokens+elapsed*tb.rate)

	if tb.tokens >= 1.0 {
		tb.tokens -= 1.0
		return true
	}

	return false
}

// IPRateLimiter manages per-client token buckets.
type IPRateLimiter struct {
	buckets sync.Map // map[string]*TokenBucket
	rate    float64
	burst   float64
}

// NewIPRateLimiter creates a new rate limiter with default rate and burst.
func NewIPRateLimiter(rate, burst float64) *IPRateLimiter {
	return &IPRateLimiter{
		rate:  rate,
		burst: burst,
	}
}

// GetBucket returns or creates the token bucket for a specific client identifier.
func (limiter *IPRateLimiter) GetBucket(key string) *TokenBucket {
	if val, ok := limiter.buckets.Load(key); ok {
		return val.(*TokenBucket)
	}
	newBucket := NewTokenBucket(limiter.rate, limiter.burst)
	actual, _ := limiter.buckets.LoadOrStore(key, newBucket)
	return actual.(*TokenBucket)
}

// RateLimitMiddleware returns a Gin middleware that enforces token bucket rate limiting.
func RateLimitMiddleware(rate, burst float64) gin.HandlerFunc {
	limiter := NewIPRateLimiter(rate, burst)

	return func(c *gin.Context) {
		// Identify client by Tenant, API Key header if present, or fallback to ClientIP
		clientID := c.GetString("tenant_id")
		if clientID == "" || clientID == "default" {
			clientID = c.GetHeader("X-API-Key")
			if clientID == "" {
				clientID = c.ClientIP()
			}
		}

		bucket := limiter.GetBucket(clientID)
		if !bucket.Allow() {
			c.Header("Retry-After", "1")
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error":               "rate limit exceeded: too many requests",
				"retry_after_seconds": 1,
			})
			return
		}

		c.Next()
	}
}

// SlidingWindowCounter implements a rate limiter using sliding windows with linear interpolation.
type SlidingWindowCounter struct {
	mu         sync.Mutex
	windowSize time.Duration
	limit      int
	currWindow int64
	currCount  int
	prevCount  int
}

// NewSlidingWindowCounter creates a new sliding window counter with given limit and window duration.
func NewSlidingWindowCounter(limit int, windowSize time.Duration) *SlidingWindowCounter {
	if windowSize <= 0 {
		windowSize = time.Minute
	}
	return &SlidingWindowCounter{
		windowSize: windowSize,
		limit:      limit,
		currWindow: time.Now().UnixNano() / int64(windowSize),
	}
}

// Allow checks if the current request is within the sliding window limit.
func (sw *SlidingWindowCounter) Allow() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()
	nowNano := now.UnixNano()
	windowIndex := nowNano / int64(sw.windowSize)

	if windowIndex > sw.currWindow {
		// Window rotated
		if windowIndex == sw.currWindow+1 {
			sw.prevCount = sw.currCount
		} else {
			sw.prevCount = 0
		}
		sw.currCount = 0
		sw.currWindow = windowIndex
	}

	// Calculate linear weight of previous window: (windowEnd - now) / windowDuration
	timeIntoCurrent := nowNano % int64(sw.windowSize)
	weight := float64(int64(sw.windowSize)-timeIntoCurrent) / float64(sw.windowSize)
	estimatedCount := float64(sw.currCount) + float64(sw.prevCount)*weight

	if estimatedCount < float64(sw.limit) {
		sw.currCount++
		return true
	}

	return false
}

