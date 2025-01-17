package config

import (
	"bytes"
	"fmt"
	"gopkg.in/yaml.v3"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type Task struct {
	Cmd   string            `yaml:"cmd"`
	Table string            `yaml:"table"`
	Where string            `yaml:"where"`
	Set   map[string]string `yaml:"set"`
	Fetch map[string]string `yaml:"fetch"`
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
	sourcePath, err := makeAbs(c.Source)
	if err != nil {
		return fmt.Errorf("source path converting error: %w", err)
	}
	c.Source = sourcePath

	destinationPath, err := makeAbs(c.Destination)
	if err != nil {
		return fmt.Errorf("destination path converting error: %w", err)
	}
	c.Destination = destinationPath
	return nil
}

func makeAbs(path string) (string, error) {
	if filepath.IsAbs(path) {
		return path, nil
	}

	if strings.HasPrefix(path, "~") {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("can't get user's home dir: %w", err)
		}

		path = strings.TrimPrefix(path, "~")
		path = strings.TrimPrefix(path, "/")
		return filepath.Join(homeDir, path), nil
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("can't get abs path: %w", err)
	}

	return absPath, nil
}
