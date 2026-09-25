//go:build integration && !windows

package integration

import "syscall"

var sigterm = syscall.SIGTERM
