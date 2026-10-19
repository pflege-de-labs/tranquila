// Package recovery turns a panic in one unit of work (a transfer job, a
// listing prefix, a bucket's discovery) into an error, so one bad object or
// prefix cannot crash the whole process. See docs/adr/0009-panic-recovery.md.
package recovery

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// The global meter delegates to the provider telemetry.Setup installs later.
var recovered, _ = otel.Meter("tranquila").Int64Counter("tranquila.panics.recovered",
	metric.WithDescription("Panics recovered per unit of work, by site"))

// Handle logs a recovered panic value r with its stack on logger, counts it
// under site, and returns it as an error. Call it from a deferred recover().
func Handle(site string, r any, logger zerolog.Logger) error {
	recovered.Add(context.Background(), 1, metric.WithAttributes(attribute.String("site", site)))
	logger.Error().Str("site", site).Interface("panic", r).Bytes("stack", debug.Stack()).
		Msg("recovered panic")
	return fmt.Errorf("panic in %s: %v", site, r)
}
