package dump

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strings"
)

type ColumnMeta struct {
	Position int
}

type TableMeta struct {
	Name          string
	Schema        string
	Columns       map[string]*ColumnMeta
	SortedColumns []string
}

func fromByteLine(line []byte) (*TableMeta, error) {
	// example: "... COPY public.test_table (id, Name, comment) FROM stdin;\n"
	table := TableMeta{}

	line = bytes.TrimSuffix(line, []byte(") FROM stdin;"))
	// don't care about line prefix
	subBytes := bytes.Split(line, []byte("COPY "))
	str := string(subBytes[len(subBytes)-1])
	substr := strings.SplitN(str, " ", 2)

	// public.test_table
	tableStr := strings.Split(substr[0], ".")
	table.Schema = tableStr[0]
	table.Name = strings.Trim(tableStr[1], "\"")

	// id, Name, comment
	columnStr := strings.Split(strings.TrimPrefix(substr[1], "("), ", ")
	table.Columns = make(map[string]*ColumnMeta, len(columnStr))
	table.SortedColumns = make([]string, 0, len(columnStr))
	for idx, columnName := range columnStr {
		table.Columns[columnName] = &ColumnMeta{
			Position: idx,
		}
		table.SortedColumns = append(table.SortedColumns, columnName)
	}
	return &table, nil
}

func LoadTableMetaFromFile(filePath string) ([]*TableMeta, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("can not open file: %w", err)
	}
	defer file.Close()

	var metadata []*TableMeta

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()
		if bytes.HasSuffix(line, []byte("FROM stdin;")) {
			table, err := fromByteLine(line)
			if err != nil {
				return nil, fmt.Errorf("can noot parse Table line: %w", err)
			}
			metadata = append(metadata, table)
		}
	}

	return metadata, nil
}
