package auth

import (
	"context"
	"testing"
)

func TestAllowAnonymousAuthorize(t *testing.T) {
	tests := []struct {
		name       string
		allowWrite bool
		op         Operation
		want       Decision
	}{
		{name: "no-write/read", allowWrite: false, op: Read, want: Allow},
		{name: "no-write/write", allowWrite: false, op: Write, want: Deny},
		{name: "allow-write/read", allowWrite: true, op: Read, want: Allow},
		{name: "allow-write/write", allowWrite: true, op: Write, want: Allow},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := AllowAnonymous{AllowWrite: tt.allowWrite}
			got := a.Authorize(context.Background(), Request{Operation: tt.op})
			if got != tt.want {
				t.Errorf("Authorize() = %v, want %v", got, tt.want)
			}
		})
	}
}
