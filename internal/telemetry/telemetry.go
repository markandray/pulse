package telemetry

import (
	"context"
	"os"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Init enables tracing when OTEL_EXPORTER_OTLP_ENDPOINT is set, and does nothing
// otherwise. The returned function flushes buffered spans and is always safe to call.
func Init(ctx context.Context, instance string) (func(context.Context) error, error) {
	nop := func(context.Context) error { return nil }
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return nop, nil
	}

	exp, err := otlptracehttp.New(ctx) // reads the endpoint from the environment
	if err != nil {
		return nop, err
	}

	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		attribute.String("service.name", "pulse"),
		attribute.String("service.instance.id", instance),
	))
	if err != nil {
		return nop, err
	}

	ratio := 1.0
	if v := os.Getenv("TRACE_SAMPLE_RATIO"); v != "" {
		if r, err := strconv.ParseFloat(v, 64); err == nil && r >= 0 && r <= 1 {
			ratio = r
		}
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		// decide once at the root span; everything downstream follows the parent
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))
	return tp.Shutdown, nil
}
