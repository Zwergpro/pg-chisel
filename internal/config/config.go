package config

import (
	"bytes"
	"fmt"
	"log"
	"os"

	"github.com/zwergpro/pg-chisel/internal/contrib/fs"
	"gopkg.in/yaml.v3"
)

type Task struct {
	Cmd   string            `yaml:"cmd"`
	Table string            `yaml:"table"`
	Where string            `yaml:"where"`
	Set   map[string]string `yaml:"set"`
	Fetch map[string]string `yaml:"fetch"`
	Type  string            `yaml:"type"`
}

type Config struct {
	Source      string `yaml:"src"`
	Destination string `yaml:"dest"`

	// So-called Table of Contents file describing the dumped objects
	// in a machine-readable format that pg_restore can read
	TocFile string `yaml:"toc"`

	// List the table of contents of the archive.
	ListFile string `yaml:"listFile"`

	Format      string `yaml:"format"`
	Compression string `yaml:"compression"`

	Storage map[string][]string `yaml:"storage"`
	Tasks   []Task              `yaml:"tasks"`
}

func New(fname string) (*Config, error) {
	c := Config{}

	log.Printf("[INFO] Read config file: %s", fname)
	data, err := os.ReadFile(fname) // nolint
	if err != nil {
		return nil, fmt.Errorf("config file %s doen't exist: %s", fname, err)
	}

	if err := unmarshalConfigFile(fname, data, &c); err != nil {
		return nil, fmt.Errorf("can't create config: %s", err)
	}

	if err := c.convertPaths(); err != nil {
		return nil, fmt.Errorf("convertPaths error: %w", err)
	}

	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("config validation error: %s", err)
	}

	return &c, nil
}

func unmarshalConfigFile(fname string, data []byte, res *Config) error {
	yamlDecoder := yaml.NewDecoder(bytes.NewReader(data))
	yamlDecoder.KnownFields(true) // strict mode, fail on unknown fields
	if err := yamlDecoder.Decode(res); err != nil {
		return fmt.Errorf("can't unmarshal yaml config %s: %w", fname, err)
	}
	return nil
}

func (c *Config) validate() error {
	return nil
}

func (c *Config) convertPaths() error {
	sourcePath, err := fs.GetAbsolutePath(c.Source)
	if err != nil {
		return fmt.Errorf("source path converting error: %w", err)
	}
	c.Source = sourcePath

	destinationPath, err := fs.GetAbsolutePath(c.Destination)
	if err != nil {
		return fmt.Errorf("destination path converting error: %w", err)
	}
	c.Destination = destinationPath
	return nil
}
