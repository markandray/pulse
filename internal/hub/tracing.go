package hub

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// tracer is looked up on every use, not cached in a package variable, so a
// provider installed after startup (or swapped in a test) is always honoured.
func tracer() trace.Tracer { return otel.Tracer("pulse/hub") }

var noopSpan = trace.SpanFromContext(context.Background())

// startSpan starts a child span only if ctx already carries a sampled trace.
// Replayed log records and unsampled events carry none, and must not each
// start a brand-new trace.
func startSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if !trace.SpanContextFromContext(ctx).IsSampled() {
		return ctx, noopSpan
	}
	return tracer().Start(ctx, name, opts...)
}
