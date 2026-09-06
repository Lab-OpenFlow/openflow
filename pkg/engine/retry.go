package engine

import (
	"math"
	"math/rand"
	"time"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
)

// CalculateBackoff computes the sleep duration for a retry attempt (1-indexed).
// Implements the Full Jitter algorithm (AWS Architecture Best Practices)
// to prevent the Thundering Herd problem when downstream systems experience outages:
//
//	Sleep = rand(0, min(MaxInterval, BaseInterval * Multiplier^(attempt-2)))
func CalculateBackoff(policy *model.RetryPolicy, attempt int) time.Duration {
	if policy == nil || attempt <= 1 {
		return 0
	}

	initial := 500 * time.Millisecond
	if policy.InitialInterval != "" {
		if d, err := time.ParseDuration(policy.InitialInterval); err == nil && d > 0 {
			initial = d
		}
	}

	var maxDuration time.Duration = 30 * time.Second
	if policy.MaxInterval != "" {
		if d, err := time.ParseDuration(policy.MaxInterval); err == nil && d > 0 {
			maxDuration = d
		}
	}

	multiplier := policy.Multiplier
	if multiplier <= 1.0 {
		multiplier = 2.0
	}

	var ceiling time.Duration
	switch policy.Backoff {
	case "constant":
		ceiling = initial
	case "linear":
		ceiling = time.Duration(float64(initial) * float64(attempt-1))
	case "exponential":
		fallthrough
	default:
		exponent := math.Pow(multiplier, float64(attempt-2))
		ceiling = time.Duration(float64(initial) * exponent)
	}

	if ceiling > maxDuration {
		ceiling = maxDuration
	}
	if ceiling <= 0 {
		return 0
	}

	// Full Jitter: sample uniformly from [initial/2, ceiling] to ensure non-zero minimum sleep while spreading load
	minSleep := initial / 2
	if minSleep > ceiling {
		minSleep = ceiling
	}

	span := float64(ceiling - minSleep)
	jittered := float64(minSleep) + (rand.Float64() * span)
	return time.Duration(jittered)
}
