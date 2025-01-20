<p align="center">
  <img alt="pg-chisel logo" src="assets/logo.png" height="150" />
  <h3 align="center">pg-chisel</h3>
  <p align="center">Chiseling away unwanted parts from the PostgreSQL dump. Instead of restoring the entire database dump, you can trim down or edit the data to quickly load only what you need.</p>
</p>

---

<div>

[![build](https://github.com/Zwergpro/pg-chisel/actions/workflows/ci.yml/badge.svg)](https://github.com/Zwergpro/pg-chisel/actions/workflows/ci.yml)

</div>


## Motivation

Sometimes your database is too large to restore on a local machine or test environment.
However, you may only need a small amount of data, such as 10% of users or something else.
`pg-chisel` allows you to trim or modify your database dump without having to fully restore it, saving time and resources.

---

## Installation

```bash
go install github.com/zwergpro/pg-chisel@latest
```
(Alternatively, download from Releases if available.)

---

## Usage

```bash
pg-chisel [OPTIONS]
```

### Options

- `-c, --config`\
  Specifies the configuration file (default is `chisel.yml`).
- `--check-config`\
  Checks the correctness of the configuration file without performing any actions.
- `-v, --verbose`\
  Enables verbose mode, providing more detailed output and error messages.
- `--dbg`\
  Enables debug mode, providing even more detailed output, error messages, and diagnostic information.
- `-h, --help`\
  Shows the help message with all available command-line options.
- `-V, --version`\
  Shows version.

---

## Basic Concepts

- **Config** is a YAML file that defines source and destination dump parameters, plus one or more tasks to be executed.
- **Task** is a unit of work.
- **Command (cmd)** is an action performed by a task (e.g., "select", "update").
- **CEL expression** is used for filtering and manipulating data (see details below).

---

## Example `chisel.yml` Configuration

```yaml

src: "./dump/src/"    # source dump directory
dest: "./dump/dst/"   # destination directory to save the new dump
toc: "toc.dat"        # Table of Contents file name
listFile: "toc.list"  # summarized TOC of the archive file name
format: "directory"   # dumps file format (directory, tar, plain text)
compression: "gzip"   # compression method can be set to gzip, lz4, zstd, or none (for no compression)

# list of predefined datasets (values must be able to be converted to a list of strings).
# you can access them from any CEL expression in the "WHERE" clause.
storage:
  profile_ids: [1, 2, 3, 4, 5]

# list of tasks, i.e. commands to execute
tasks:

  # select table.id into users_to_save
  # from users as table
  # where string(table.profile_id) in array("profile_ids")
  - cmd: "select"  # command type
    table: "users"  # table name
    fetch:  # list of fields to be selected and stored the global storage
      users_to_save: "table.id"  # storage_key: "CEL expression"
    where: 'string(table.profile_id) in array("profile_ids")'  # CEL filter expression

  # update reviews as table
  # set user_id = null
  # where string(table.user_id) in set("users_to_save")
  - cmd: "update"
    table: "reviews"
    set:  # list of table's fields to be modified
      user_id: 'NULL'
    where: 'string(table.user_id) in set("users_to_save")'

  # delete from comments as table
  # where string(table.author_id) not in set("users_to_save")
  - cmd: "delete"
    table: "comments"
    where: '!(string(table.author_id) in set("users_to_save"))'

  # "copy" missing files from src to dest
  - cmd: "sync"
    type: "hard_link"  # sync type: copy or hard_link

  # remove all table data
  - cmd: "truncate"
    table: "users"
```

#### Supported values

More information in pg_dump [documentation](https://www.postgresql.org/docs/current/app-pgdump.html)

- **format**
  - Supported: `directory`
  - Not supported: `tar`, `plain`, `custom`

- **compression**
    - Supported: `gzip`
    - Not supported: `lz4`, `zstd`, `none`

---

## CEL expression

[CEL (Common Expression Language)](https://cel.dev/) allows flexible filtering and manipulation of dump data.\
It provides a high-level language for filtering and manipulating rows within the PostgreSQL dump.
`pg-chisel` lets you write expressions that determine which rows to keep, update, or delete, and also helps you store “fetched” values for later use in subsequent tasks.

For more detail see [CEL Language Definition](https://github.com/google/cel-spec/blob/master/doc/langdef.md).

#### Key features of using CEL:

- **Flexible Filtering**: Write expressions such as `string(table.id) in set("users_to_save")` to filter rows dynamically.
- **Data Manipulation**: Modify column values on the fly (e.g., set them to `NULL`) using expressions like `NULL` or `string(table.author_id) + "_user@email.com"`.
- **Chaining Commands**: The results (storage) from one command (e.g., “select”) can be used in the `where` clause of another command (e.g., “update” or “delete”).

#### Data available in each expression:

- `table`: The current database record, a `map[string][]byte`. Access columns as `table.column_name`, and convert them to a suitable type (e.g., `string(table.id)`).
- `NULL`: A constant representing the PostgreSQL “null” string (`"\N"`).
- `Global storage`: A dictionary of lists or sets populated by previous commands. You can reference it via custom functions array("key") or set("key").

#### Custom Functions:

- `array(storage_key string)`\
  Returns the list of strings associated with `storage_key`. This is useful when you need to do “one-of” membership checks or iterate over values.
- `set(storage_key string)`\
  Returns the set of strings (as a `map[string]struct{}`) associated with `storage_key`. Ideal for large membership checks, since `x in set("users_to_save")` can be more efficient than list iteration.


#### Typical Usage:
- Filter rows to be selected, updated, or deleted by putting a where clause with a CEL expression.
- Convert fields from []byte to a specific type (string, int, etc.) if needed:
```cel
string(table.id) == NULL
int(string(table.profile_id)) == 3
string(table.id) in array("profile_ids")
string(table.author_id) in set("users_to_save")
```
- Save results (like a user ID) into global storage to use them in a subsequent command’s `where` clause.

*Note: CEL expressions are case-sensitive*

---

## Command Types

A task corresponds to one command applied to a given table or filesystem resource. Tasks are run in the order they appear in the config.

#### `select`

**Operation**: Iterates over all rows in a specified table, evaluates a CEL `where` expression to determine which rows to process,
and then fetches data from those rows into a global storage map for future tasks.

```yaml
  - cmd: "select"
    table: "users"
    fetch:
      users_to_save: "table.id"
    where: 'string(table.profile_id) in array("profile_ids")'
```

- **fetch**: A dictionary of `storage_key: "CEL expression"` pairs. For each matching row, `CEL expression` is evaluated, converted to string, and appended to the global storage list under `storage_key`.

#### `update`

**Operation**: Iterates over all rows in a specified table, uses a CEL `where` expression to determine which rows to modify, then applies changes in the dump data.

```yaml
  - cmd: "update"
    table: "reviews"
    set:
      user_id: 'NULL'
    where: '!(string(table.user_id) in set("users_to_save"))'

  - cmd: "update"
    table: "users"
    set:
      fullname: 'string(table.first_name) + " " + string(table.last_name)'
    where: 'string(table.id) in set("users_to_save")'
```

- **set**: A dictionary of `column_name: "CEL expression"` pairs. If a row matches `where`, each `CEL expression` is evaluated and replaced in that column (e.g., setting it to `NULL`).


#### `delete`

**Operation**: Iterates over all rows in a specified table, evaluating a CEL `where` expression. Matching rows are removed from the final output dump.

```yaml
  - cmd: "delete"
    table: "comments"
    where: '!(string(table.author_id) in set("users_to_save"))'
```

- If `where` evaluates to true for a row, that row is excluded from the dump.

#### `sync`

**Operation**: Synchronizes files between the `src` and `dest` directories (as defined in your config). This can be done via copy or hard-link, ensuring any needed files not yet processed by other tasks end up in the destination.

```yaml
  - cmd: "sync"
    type: "hard_link"
```

- **type**: `copy` or `hard_link`. This controls how files are transferred (e.g., physically copying vs. creating filesystem links).

#### `truncate`

**Operation**: Truncates the contents of a specified table in the dump file. This effectively clears the table's data in the output without removing the table structure.

```yaml
  - cmd: "truncate"
    table: "users"
```

- **table**: The name of the table to truncate. The structure of the table is preserved in the dump file, but its rows are removed.

---

## Status

The project is currently undergoing active development, and there may be breaking changes until the release of version 1.0

---


## Contributing

If you have any questions or suggestions, please feel free to start a discussion, submit an issue, fork the repository, or send a pull request. For more information, see the [CONTRIBUTING.md](https://github.com/Zwergpro/pg-chisel/blob/main/CONTRIBUTING.md).

---

## License

This project is licensed under the MIT license. For more details, please see the LICENSE file.
