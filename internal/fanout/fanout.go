package fanout

import (
	"context"

	"github.com/synamedia/linear-packager/internal/segment"
)

// FanOut reads from src and writes each segment to n output channels in order.
// All output channels are closed when src is closed or ctx is cancelled.
// Use a buffer size of 64 per consumer so a slow consumer doesn't stall others.
func FanOut(ctx context.Context, src <-chan segment.Segment, n int) []<-chan segment.Segment {
	outs := make([]chan segment.Segment, n)
	for i := range outs {
		outs[i] = make(chan segment.Segment, 64)
	}

	go func() {
		defer func() {
			for _, ch := range outs {
				close(ch)
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case seg, ok := <-src:
				if !ok {
					return
				}
				for _, ch := range outs {
					select {
					case ch <- seg:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	result := make([]<-chan segment.Segment, n)
	for i, ch := range outs {
		result[i] = ch
	}
	return result
}
