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
// Both are swappable because a sweep changes the payload size between steps
// and shrinks the keyspace to keep the dataset bounded when values get large;
// the swaps are atomic so a step that is still draining never sees a torn value.
type Workload struct {
	payload  atomic.Pointer[[]byte]
	keyspace atomic.Int64
}

// Marker is the one-byte value behind every stat key on the Dapr side.
var Marker = []byte{'1'}

// NewWorkload builds a workload with a payload of the requested size.
func NewWorkload(payloadBytes, keyspace int) *Workload {
	w := &Workload{}
	w.SetPayloadBytes(payloadBytes)
	w.SetKeyspace(keyspace)
	return w
}

// SetPayloadBytes replaces the payload with one of the given size.
func (w *Workload) SetPayloadBytes(n int) {
	if n < 1 {
		n = 1
	}

	payload := make([]byte, n)
	for i := range payload {
		// printable, non-uniform bytes so compression in any layer cannot
		// flatter one backend over another. The range includes characters
		// outside the base64 alphabet on purpose: Dapr's localstorage binding
		// base64-decodes any data that happens to decode, which would silently
		// shrink an all-letters payload by a quarter.
		payload[i] = byte(33 + i%94)
	}

	w.payload.Store(&payload)
}

// SetKeyspace sets how many distinct keys the connectors cycle through.
func (w *Workload) SetKeyspace(n int) {
	if n < 1 {
		n = 1
	}
	w.keyspace.Store(int64(n))
}

// Payload is the value written on every heavy operation.
func (w *Workload) Payload() []byte { return *w.payload.Load() }

// PayloadBytes is the current payload size.
func (w *Workload) PayloadBytes() int { return len(w.Payload()) }

// Keyspace is the current number of distinct keys.
func (w *Workload) Keyspace() int { return int(w.keyspace.Load()) }

// Key maps an iteration number onto the bounded keyspace.
func (w *Workload) Key(i int) string {
	return fmt.Sprintf("bench-key-%d", i%w.Keyspace())
}

// MarkerKey is the stat key for an iteration: a separate, tiny record that
// exists so a metadata-style call never has to move the payload.
func (w *Workload) MarkerKey(i int) string {
	return fmt.Sprintf("bench-meta-%d", i%w.Keyspace())
}
