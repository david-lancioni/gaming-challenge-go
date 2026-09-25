package application

import (
	"math/rand/v2"
	"time"
)

func Backoff(base, max time.Duration, attempt int) time.Duration {
	if base <= 0 {
		return 0
	}
	if attempt < 0 {
		attempt = 0
	}
	d := base
	for i := 0; i < attempt && d < max; i++ {
		d *= 2
	}
	if max > 0 && d > max {
		d = max
	}
	jitter := 0.8 + rand.Float64()*0.4
	return time.Duration(float64(d) * jitter)
}
