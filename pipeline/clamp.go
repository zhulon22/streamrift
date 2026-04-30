package pipeline

import "context"

// ClampStage clamps numeric values to the range [min, max].
// Values below min are replaced with min; values above max are replaced with max.
func ClampStage[N interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}](ctx context.Context, in <-chan N, min, max N) <-chan N {
	out := make(chan N)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-in:
				if !ok {
					return
				}
				if v < min {
					v = min
				} else if v > max {
					v = max
				}
				select {
				case out <- v:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// NormalizeStage maps numeric values from [srcMin, srcMax] into [dstMin, dstMax].
// Values outside the source range are clamped before mapping.
func NormalizeStage[N interface {
	~float32 | ~float64
}](ctx context.Context, in <-chan N, srcMin, srcMax, dstMin, dstMax N) <-chan N {
	out := make(chan N)
	go func() {
		defer close(out)
		srcRange := srcMax - srcMin
		dstRange := dstMax - dstMin
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-in:
				if !ok {
					return
				}
				if v < srcMin {
					v = srcMin
				} else if v > srcMax {
					v = srcMax
				}
				var mapped N
				if srcRange == 0 {
					mapped = dstMin
				} else {
					mapped = (v-srcMin)/srcRange*dstRange + dstMin
				}
				select {
				case out <- mapped:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}
