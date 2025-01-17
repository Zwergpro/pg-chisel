package dump

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type EntityDescType string

const (
	COMMENT           EntityDescType = "COMMENT"
	CONSTRAINT        EntityDescType = "CONSTRAINT"
	DEFAULT           EntityDescType = "DEFAULT"
	EXTENSION         EntityDescType = "EXTENSION"
	FK_CONSTRAINT     EntityDescType = "FK CONSTRAINT"
	FUNCTION          EntityDescType = "FUNCTION"
	INDEX             EntityDescType = "INDEX"
	SEQUENCE          EntityDescType = "SEQUENCE"
	SEQUENCE_OWNED_BY EntityDescType = "SEQUENCE OWNED BY"
	SEQUENCE_SET      EntityDescType = "SEQUENCE SET"
	TABLE             EntityDescType = "TABLE"
	TABLE_DATA        EntityDescType = "TABLE DATA"
	TRIGGER           EntityDescType = "TRIGGER"
)

type EntityMeta struct {
	DumpId   int
	TableOID int
	OID      int
	Desc     EntityDescType
	Schema   string
	Table    string
	Name     string
	Owner    string
}

func (m *EntityMeta) fromLine(line string) error {
	// https://github.com/postgres/postgres/blob/master/src/bin/pg_dump/pg_backup_archiver.c#L1340
	parts := strings.SplitN(line, " ", 4)
	// TODO: validate parts

	// parse common prefix part
	val, err := strconv.Atoi(strings.TrimSuffix(parts[0], ";"))
	if err != nil {
		return fmt.Errorf("DumpId parse error: %w", err)
	}
	m.DumpId = val

	val, err = strconv.Atoi(parts[1])
	if err != nil {
		return fmt.Errorf("TableOID parse error: %w", err)
	}
	m.TableOID = val

	val, err = strconv.Atoi(parts[2])
	if err != nil {
		return fmt.Errorf("OID parse error: %w", err)
	}
	m.OID = val

	if err = m.parseVariablePart(parts[3]); err != nil {
		return fmt.Errorf("variable part parse error: %w", err)
	}

	return nil
}

func (m *EntityMeta) parseVariablePart(variablePart string) error {
	switch {
	case strings.HasPrefix(variablePart, string(COMMENT)):
		// example: "COMMENT - EXTENSION pg_stat_statements"
		m.Desc = COMMENT
		parts := strings.SplitN(variablePart, " ", 3)
		if len(parts) < 3 {
			return errors.New("invalid COMMENT line")
		}
		m.Name = parts[2]
	case strings.HasPrefix(variablePart, string(CONSTRAINT)):
		// example: "CONSTRAINT public test_table test_table_pkey user"
		m.Desc = CONSTRAINT
		parts := strings.Split(variablePart, " ")
		if len(parts) != 5 {
			return errors.New("invalid CONSTRAINT line")
		}
		m.setTail(parts[1:])
	case strings.HasPrefix(variablePart, string(DEFAULT)):
		// example: "DEFAULT public test_table id user"
		m.Desc = DEFAULT
		parts := strings.Split(variablePart, " ")
		if len(parts) != 5 {
			return errors.New("invalid DEFAULT line")
		}
		m.setTail(parts[1:])
	case strings.HasPrefix(variablePart, string(EXTENSION)):
		// example: "EXTENSION - pg_stat_statements"
		m.Desc = EXTENSION
		parts := strings.SplitN(variablePart, " ", 3)
		m.Name = parts[2]
	case strings.HasPrefix(variablePart, string(FK_CONSTRAINT)):
		// example: "FK CONSTRAINT public test_table test_table_column_id_84c5c92e_fk_perm user"
		m.Desc = FK_CONSTRAINT
		parts := strings.Split(variablePart, " ")
		if len(parts) != 6 {
			return errors.New("invalid FK_CONSTRAINT line")
		}
		m.setTail(parts[2:])
	case strings.HasPrefix(variablePart, string(FUNCTION)):
		m.Desc = FUNCTION
		// WARNING: we need to remember that we split line by space
		// example: FUNCTION public test_func(jsonb, jsonb) user
		// example: FUNCTION public test_func() user
		parts := strings.Split(variablePart, " ")
		if len(parts) < 4 {
			return errors.New("invalid FUNCTION line")
		}
		m.Schema = parts[1]
		m.Name = strings.Join(parts[2:len(parts)-1], " ")
		m.Owner = parts[len(parts)-1]
	case strings.HasPrefix(variablePart, string(INDEX)):
		// example: "INDEX public test_table_1eba186c user"
		m.Desc = INDEX
		parts := strings.Split(variablePart, " ")
		if len(parts) != 4 {
			return errors.New("invalid INDEX line")
		}
		m.setTail(parts[1:])

	// SEQUENCE variablePart
	case strings.HasPrefix(variablePart, string(SEQUENCE_OWNED_BY)):
		// example: "SEQUENCE OWNED BY public test_table_id_seq user"
		m.Desc = SEQUENCE_OWNED_BY
		parts := strings.Split(variablePart, " ")
		if len(parts) != 6 {
			return errors.New("invalid SEQUENCE_OWNED_BY line")
		}
		m.setTail(parts[3:])
	case strings.HasPrefix(variablePart, string(SEQUENCE_SET)):
		// example: "SEQUENCE SET public test_table_id_seq user"
		m.Desc = SEQUENCE_SET
		parts := strings.Split(variablePart, " ")
		if len(parts) != 5 {
			return errors.New("invalid SEQUENCE_SET line")
		}
		m.setTail(parts[2:])
	case strings.HasPrefix(variablePart, string(SEQUENCE)):
		// example: "SEQUENCE public test_table_id_seq user"
		m.Desc = SEQUENCE
		parts := strings.Split(variablePart, " ")
		if len(parts) != 4 {
			return errors.New("invalid SEQUENCE line")
		}
		m.setTail(parts[1:])

	// Table variablePart
	case strings.HasPrefix(variablePart, string(TABLE_DATA)):
		// example: "TABLE DATA public test_table user"
		m.Desc = TABLE_DATA
		parts := strings.Split(variablePart, " ")
		if len(parts) != 5 {
			return errors.New("invalid TABLE_DATA line")
		}
		m.setTail(parts[2:])
	case strings.HasPrefix(variablePart, string(TABLE)):
		// example: "TABLE public test_table user"
		m.Desc = TABLE
		parts := strings.Split(variablePart, " ")
		if len(parts) != 4 {
			return errors.New("invalid TABLE line")
		}
		m.setTail(parts[1:])

	case strings.HasPrefix(variablePart, string(TRIGGER)):
		// example: "TRIGGER public test_table test_table_update_trg user"
		m.Desc = TRIGGER
		parts := strings.Split(variablePart, " ")
		if len(parts) != 5 {
			return errors.New("invalid TRIGGER line")
		}
		m.setTail(parts[1:])
	}

	return nil
}

func (m *EntityMeta) setTail(tail []string) {
	m.Schema = tail[0]
	if len(tail) > 3 {
		m.Table = tail[1]
	}
	m.Name = tail[len(tail)-2]
	m.Owner = tail[len(tail)-1]
}

func LoadEntityMetaFromFile(filePath string) ([]*EntityMeta, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("can not open file: %w", err)
	}
	defer file.Close()

	var metadata []*EntityMeta

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, ";") {
			continue // skip comment
		}

		meta, err := EntityMetaFromLine(line)
		if err != nil {
			return nil, fmt.Errorf("parsing file error: %w", err)
		}
		metadata = append(metadata, meta)
	}

	return metadata, nil
}

func EntityMetaFromLine(line string) (*EntityMeta, error) {
	meta := EntityMeta{}
	err := meta.fromLine(line)
	if err != nil {
		return nil, fmt.Errorf("can not parse meta data line: %w", err)
	}
	return &meta, nil
}
