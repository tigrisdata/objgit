package main

import (
	"crypto/rand"
	"fmt"
)

// newUUID returns a random version 4 UUID. The harness only needs repository
// names that never collide with an earlier run, so crypto/rand covers it and no
// dependency is needed.
func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("membench: can't read random bytes: %w", err)
	}

	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
