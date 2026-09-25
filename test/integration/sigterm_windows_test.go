//go:build integration && windows

package integration

import "os"

var sigterm os.Signal = os.Kill
