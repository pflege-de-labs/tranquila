package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/alecthomas/kong"
	kongyaml "github.com/alecthomas/kong-yaml"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var version = "dev"

type CLI struct {
	ConfigFile       kong.ConfigFlag  `name:"configFile" short:"c" help:"Full path to a user-supplied config file"`
	LogLevel         string           `name:"log-level" help:"Log level (trace,debug,info,warn,error)" env:"TRANQUILA_LOG_LEVEL" default:"info"`
	LogJSON          bool             `name:"log-json" help:"Output logs as JSON" env:"TRANQUILA_LOG_JSON"`
	MemLimitRatio    float64          `name:"memlimit-ratio" help:"Ratio of the detected memory limit to set as GOMEMLIMIT" env:"TRANQUILA_MEMLIMIT_RATIO" default:"0.9"`
	MemLimitProvider string           `name:"memlimit-provider" help:"Memory limit provider (cgroupv1/cgroupv2 are deprecated aliases for cgroup, which now auto-detects the version)" enum:"cgroup,cgroupv1,cgroupv2,system" env:"TRANQUILA_MEMLIMIT_PROVIDER" default:"cgroup"`
	MemLimitRefresh  time.Duration    `name:"memlimit-refresh-interval" help:"Interval to refresh GOMEMLIMIT from the provider (0 disables refresh)" env:"TRANQUILA_MEMLIMIT_REFRESH_INTERVAL" default:"0s"`
	Version          kong.VersionFlag `name:"version" short:"V" help:"Show version and exit"`
	Sync             SyncCmd          `cmd:"" default:"" help:"Synchronize S3 buckets (default)"`
	Status           StatusCmd        `cmd:"" help:"Show synchronization status"`
	Completion       CompletionCmd    `cmd:"" help:"Generate a shell completion script"`
}

// buildParser constructs the kong parser for cli, loading YAML config from cfgPaths in order.
// Exported for test reuse.
func buildParser(cli *CLI, cfgPaths ...string) *kong.Kong {
	return kong.Must(cli,
		kong.Configuration(envFirst(kongyaml.Loader), cfgPaths...),
		kong.Name("tranquila"),
		kong.Description("S3 bucket synchronization tool"),
		kong.Vars{"version": version},
		kong.ConfigureHelp(kong.HelpOptions{Compact: true}),
	)
}

// envFirst makes config-file values yield to a set env var. kong applies env in
// Value.Reset, before resolvers, so a plain file resolver would overwrite it.
func envFirst(loader kong.ConfigurationLoader) kong.ConfigurationLoader {
	return func(r io.Reader) (kong.Resolver, error) {
		res, err := loader(r)
		if err != nil {
			return nil, err
		}
		return envFirstResolver{res}, nil
	}
}

type envFirstResolver struct{ kong.Resolver }

func (e envFirstResolver) Resolve(kctx *kong.Context, parent *kong.Path, flag *kong.Flag) (any, error) {
	for _, env := range flag.Envs {
		// LookupEnv, not Getenv: kong treats a set-but-empty var as set.
		if _, ok := os.LookupEnv(env); ok {
			return nil, nil
		}
	}
	return e.Resolver.Resolve(kctx, parent, flag)
}

func main() {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.Fatal().Err(err).Msg("unable to resolve home directory")
	}

	cli := &CLI{}
	parser := buildParser(cli,
		"tranquila.yaml",
		filepath.Join(homeDir, ".config", "tranquila.yaml"),
		"/etc/tranquila/tranquila.yaml",
	)

	kctx, err := parser.Parse(os.Args[1:])
	parser.FatalIfErrorf(err)

	setupLogging(cli.LogLevel, cli.LogJSON)
	setupMemLimit(cli)

	if err := kctx.Run(); err != nil {
		log.Fatal().Err(err).Msg("command failed")
	}
}

func setupMemLimit(cli *CLI) {
	if runtime.GOOS == "darwin" {
		if memLimitConfigured(cli) {
			log.Warn().Msg("memlimit configuration is not supported on darwin (no cgroups); ignoring")
		}
		return
	}

	logger := slog.New(zerolog.NewSlogHandler(log.Logger))
	if _, err := memlimit.Set(
		memlimit.WithLogger(logger),
		memlimit.WithRatio(cli.MemLimitRatio),
		memlimit.WithProvider(memLimitProvider(cli.MemLimitProvider)),
		memlimit.WithRefreshInterval(context.Background(), cli.MemLimitRefresh),
	); err != nil {
		log.Warn().Err(err).Msg("failed to set GOMEMLIMIT")
	}
}

// memLimitConfigured reports whether the user set any memlimit flag away from its default.
func memLimitConfigured(cli *CLI) bool {
	return cli.MemLimitRatio != 0.9 || cli.MemLimitProvider != "cgroup" || cli.MemLimitRefresh != 0
}

// memLimitProvider returns the automemlimit Provider for name. automemlimit
// v1.0.0 removed FromCgroupV1/FromCgroupV2 in favor of FromCgroup's built-in
// cgroup v1/v2 auto-detection — forcing a specific cgroup version is no
// longer possible, so cgroupv1/cgroupv2 fall back to auto-detection.
func memLimitProvider(name string) memlimit.Provider {
	switch name {
	case "cgroupv1", "cgroupv2":
		log.Warn().Str("memlimit-provider", name).
			Msg("forcing a specific cgroup version is no longer supported (automemlimit v1.0.0 removed it); using cgroup auto-detection")
		return memlimit.FromCgroup
	case "system":
		return memlimit.FromSystem
	default:
		return memlimit.FromCgroup
	}
}

func setupLogging(level string, jsonOutput bool) {
	if !jsonOutput {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		lvl = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(lvl)
}
