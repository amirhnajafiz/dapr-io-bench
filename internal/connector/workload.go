// workload.go
package connector

import (
	"fmt"
	"sync/atomic"
)

// Workload is the payload and keyspace handed to every connector. Sharing one
// instance guarantees that the only variable between two results is the data
// path, never the amount of data moved.
//
// The payload is swappable because a sweep changes its size between steps;
// the swap is atomic so a step that is still draining never sees a torn value.
type Workload struct {
	payload  atomic.Pointer[[]byte]
	Keyspace int // keys are cycled modulo this, keeping the dataset bounded
}

// NewWorkload builds a workload with a payload of the requested size.
func NewWorkload(payloadBytes, keyspace int) *Workload {
	if keyspace < 1 {
		keyspace = 1
	}

	w := &Workload{Keyspace: keyspace}
	w.SetPayloadBytes(payloadBytes)

	return w
}

// SetPayloadBytes replaces the payload with one of the given size.
func (w *Workload) SetPayloadBytes(n int) {
	if n < 1 {
		n = 1
	}

	payload := make([]byte, n)
	for i := range payload {
		// non-zero, non-uniform bytes so compression in any layer cannot
		// flatter one backend over another
		payload[i] = byte('a' + i%26)
	}

	w.payload.Store(&payload)
}

// Payload is the value written on every operation.
func (w *Workload) Payload() []byte { return *w.payload.Load() }

// PayloadBytes is the current payload size.
func (w *Workload) PayloadBytes() int { return len(w.Payload()) }

// Key maps an iteration number onto the bounded keyspace.
func (w *Workload) Key(i int) string {
	return fmt.Sprintf("bench-key-%d", i%w.Keyspace)
}
