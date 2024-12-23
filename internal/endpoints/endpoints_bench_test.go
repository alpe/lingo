package endpoints

import (
	"context"
	"testing"
)

func BenchmarkEndpointGroup(b *testing.B) {
	e := newEndpointGroup()
	e.setAddrs(map[string]endpointAttrs{"10.0.0.1": {}})
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, f, err := e.getBestAddr(context.Background(), "", false)
			if err != nil {
				b.Fatal(err)
			}
			f()
		}
	})
}
