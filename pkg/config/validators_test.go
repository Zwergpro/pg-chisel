package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateConfig_General(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: GZIP_COMPRESSION,
			Tasks: []Task{
				{
					Cmd:   "select",
					Table: "users",
					Where: "int(string(table.id)) > 0",
					Fetch: map[string]string{"id": "table.id"},
				},
			},
		}

		err := ValidateConfig(conf)
		require.NoError(t, err)
	})

	t.Run("empty destination", func(t *testing.T) {
		conf := &Config{
			Source: "src",
		}

		err := ValidateConfig(conf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "destination can not be empty")
	})

	t.Run("invalid format", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      "invalid_format",
		}

		err := ValidateConfig(conf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported format")
	})

	t.Run("invalid compression", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: "invalid_compression",
		}

		err := ValidateConfig(conf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported compression")
	})

	t.Run("empty tasks", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: GZIP_COMPRESSION,
		}

		err := ValidateConfig(conf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "tasks cannot be empty")
	})

	t.Run("invalid task cmd", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: GZIP_COMPRESSION,
			Tasks: []Task{
				{Cmd: "invalid_cmd"},
			},
		}

		err := ValidateConfig(conf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported cmd")
	})
}

func TestValidateConfig_SelectCmd(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: GZIP_COMPRESSION,
			Tasks: []Task{
				{
					Cmd:   "select",
					Table: "users",
					Where: "int(string(table.id)) > 0",
					Fetch: map[string]string{"id": "table.id"},
				},
			},
		}

		err := ValidateConfig(conf)
		require.NoError(t, err)
	})

	t.Run("missing table in task", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: GZIP_COMPRESSION,
			Tasks: []Task{
				{
					Cmd: "select",
				},
			},
		}

		err := ValidateConfig(conf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "'table' cannot be empty")
	})

	t.Run("missing where in select task", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: GZIP_COMPRESSION,
			Tasks: []Task{
				{
					Cmd:   "select",
					Table: "users",
				},
			},
		}

		err := ValidateConfig(conf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "'where' expression cannot be empty")
	})

	t.Run("missing Fetch in task", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: GZIP_COMPRESSION,
			Tasks: []Task{
				{
					Cmd:   "select",
					Table: "users",
					Where: "int(string(table.id)) > 1",
				},
			},
		}

		err := ValidateConfig(conf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "'fetch' cannot be empty")
	})

	t.Run("invalid CEL expression in where clause", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: GZIP_COMPRESSION,
			Tasks: []Task{
				{
					Cmd:   "select",
					Table: "users",
					Where: "invalid_expr",
					Fetch: map[string]string{"id": "id"},
				},
			},
		}

		err := ValidateConfig(conf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "checking error")
	})

	t.Run("invalid CEL expression in fetch expression", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: GZIP_COMPRESSION,
			Tasks: []Task{
				{
					Cmd:   "select",
					Table: "users",
					Where: "int(string(table.id)) > 1",
					Fetch: map[string]string{"id": `1 + "12"`},
				},
			},
		}

		err := ValidateConfig(conf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "checking error")
	})
}

func TestValidateConfig_UpdateCmd(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: GZIP_COMPRESSION,
			Tasks: []Task{
				{
					Cmd:   "update",
					Table: "users",
					Set:   map[string]string{"id": "table.id"},
					Where: "int(string(table.id)) > 0",
				},
			},
		}

		err := ValidateConfig(conf)
		require.NoError(t, err)
	})

	t.Run("missing table in task", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: GZIP_COMPRESSION,
			Tasks: []Task{
				{
					Cmd: "update",
				},
			},
		}

		err := ValidateConfig(conf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "'table' cannot be empty")
	})

	t.Run("missing where in select task", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: GZIP_COMPRESSION,
			Tasks: []Task{
				{
					Cmd:   "update",
					Table: "users",
				},
			},
		}

		err := ValidateConfig(conf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "'where' expression cannot be empty")
	})

	t.Run("missing Set in task", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: GZIP_COMPRESSION,
			Tasks: []Task{
				{
					Cmd:   "update",
					Table: "users",
					Where: "int(string(table.id)) > 1",
				},
			},
		}

		err := ValidateConfig(conf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "'set' cannot be empty")
	})

	t.Run("invalid CEL expression in where clause", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: GZIP_COMPRESSION,
			Tasks: []Task{
				{
					Cmd:   "update",
					Table: "users",
					Where: "invalid_expr",
					Fetch: map[string]string{"id": "id"},
				},
			},
		}

		err := ValidateConfig(conf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "checking error")
	})

	t.Run("invalid CEL expression in set expression", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: GZIP_COMPRESSION,
			Tasks: []Task{
				{
					Cmd:   "update",
					Table: "users",
					Where: "int(string(table.id)) > 1",
					Set:   map[string]string{"id": `1 + "12"`},
				},
			},
		}

		err := ValidateConfig(conf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "checking error")
	})
}

func TestValidateConfig_DeleteCmd(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: GZIP_COMPRESSION,
			Tasks: []Task{
				{
					Cmd:   "delete",
					Table: "users",
					Where: "int(string(table.id)) > 0",
				},
			},
		}

		err := ValidateConfig(conf)
		require.NoError(t, err)
	})

	t.Run("missing table in task", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: GZIP_COMPRESSION,
			Tasks: []Task{
				{
					Cmd: "delete",
				},
			},
		}

		err := ValidateConfig(conf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "'table' cannot be empty")
	})

	t.Run("missing where in select task", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: GZIP_COMPRESSION,
			Tasks: []Task{
				{
					Cmd:   "delete",
					Table: "users",
				},
			},
		}

		err := ValidateConfig(conf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "'where' expression cannot be empty")
	})

	t.Run("invalid CEL expression in where clause", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: GZIP_COMPRESSION,
			Tasks: []Task{
				{
					Cmd:   "delete",
					Table: "users",
					Where: "invalid_expr",
				},
			},
		}

		err := ValidateConfig(conf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "checking error")
	})
}

func TestValidateConfig_SyncCmd(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: GZIP_COMPRESSION,
			Tasks: []Task{
				{
					Cmd:  "sync",
					Type: "copy",
				},
			},
		}

		err := ValidateConfig(conf)
		require.NoError(t, err)
	})

	t.Run("missing type in task", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: GZIP_COMPRESSION,
			Tasks: []Task{
				{
					Cmd: "sync",
				},
			},
		}

		err := ValidateConfig(conf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "'type' cannot be empty")
	})

	t.Run("invalid type in task", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: GZIP_COMPRESSION,
			Tasks: []Task{
				{
					Cmd:  "sync",
					Type: "invalid_type",
				},
			},
		}

		err := ValidateConfig(conf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "'type' has invalid value")
	})
}

func TestValidateConfig_TruncateCmd(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: GZIP_COMPRESSION,
			Tasks: []Task{
				{
					Cmd:   "truncate",
					Table: "users",
				},
			},
		}

		err := ValidateConfig(conf)
		require.NoError(t, err)
	})

	t.Run("missing table in task", func(t *testing.T) {
		conf := &Config{
			Source:      "src",
			Destination: "dest",
			Format:      DIRECTORY_FORMAT,
			Compression: GZIP_COMPRESSION,
			Tasks: []Task{
				{
					Cmd: "truncate",
				},
			},
		}

		err := ValidateConfig(conf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "'table' cannot be empty")
	})
}
