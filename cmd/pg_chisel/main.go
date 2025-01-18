package main

import (
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/fatih/color"
	"github.com/go-pkgz/lgr"
	"github.com/jessevdk/go-flags"
	"github.com/zwergpro/pg-chisel/internal/chisel/storage"
	"github.com/zwergpro/pg-chisel/internal/chisel/strategies"
	"github.com/zwergpro/pg-chisel/internal/config"
	"github.com/zwergpro/pg-chisel/internal/dump"
)

var opts struct {
	Verbose bool `short:"v" long:"verbose" description:"Show verbose debug information"`
}

func setupLog(dbg bool, secs ...string) {
	logOpts := []lgr.Option{lgr.Out(io.Discard), lgr.Err(io.Discard)} // default to discard
	if dbg {
		logOpts = []lgr.Option{lgr.Debug, lgr.Msec, lgr.LevelBraces, lgr.StackTraceOnError}
	}

	colorizer := lgr.Mapper{
		ErrorFunc:  func(s string) string { return color.New(color.FgHiRed).Sprint(s) },
		WarnFunc:   func(s string) string { return color.New(color.FgRed).Sprint(s) },
		InfoFunc:   func(s string) string { return color.New(color.FgYellow).Sprint(s) },
		DebugFunc:  func(s string) string { return color.New(color.FgWhite).Sprint(s) },
		CallerFunc: func(s string) string { return color.New(color.FgBlue).Sprint(s) },
		TimeFunc:   func(s string) string { return color.New(color.FgCyan).Sprint(s) },
	}
	logOpts = append(logOpts, lgr.Map(colorizer))
	if len(secs) > 0 {
		logOpts = append(logOpts, lgr.Secret(secs...))
	}
	lgr.SetupStdLogger(logOpts...)
	lgr.Setup(logOpts...)
}

func main() {
	p := flags.NewParser(&opts, flags.PrintErrors|flags.PassDoubleDash|flags.HelpFlag)
	p.SubcommandsOptional = true
	if _, err := p.Parse(); err != nil {
		if !errors.Is(err.(*flags.Error).Type, flags.ErrHelp) {
			log.Printf("[ERROR] cli error: %v", err)
		}
		os.Exit(2)
	}

	setupLog(opts.Verbose)

	log.Printf("[DEBUG] options: %+v", opts)

	err := run()
	if err != nil {
		log.Fatalf("[ERROR] error occurred: %v", err)
	}
}

func run() error {
	setupLog(true)

	log.Printf("[INFO] Start dump modification")
	confPath, err := filepath.Abs("config_big.yml")
	if err != nil {
		return err
	}

	conf, err := config.New(confPath)
	if err != nil {
		return err
	}
	log.Printf("[INFO] Source dir: %s", conf.Source)
	log.Printf("[INFO] Destination dir: %s", conf.Destination)

	dbDump, err := dump.LoadDump(conf)
	if err != nil {
		return err
	}

	globalStorage, err := storage.NewMapStringStorage(conf.Storage)
	if err != nil {
		return err
	}

	strategy, err := strategies.BuildConsistentStrategy(conf, dbDump, globalStorage)
	if err != nil {
		return err
	}

	if err = strategy.Execute(); err != nil {
		return err
	}

	return nil
}
