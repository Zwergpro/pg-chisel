package dump

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEntityMetaLongFromLine(t *testing.T) {
	testCases := []struct {
		line  string
		desc  EntityDescType
		table string
		name  string
	}{
		{
			"22; 2606 63816822 CONSTRAINT public test_table test_table_pkey user",
			CONSTRAINT,
			"test_table",
			"test_table_pkey",
		},
		{
			"22; 2606 63816822 FK CONSTRAINT public test_table test_table_column_id_84c5c92e_fk_perm user",
			FK_CONSTRAINT,
			"test_table",
			"test_table_column_id_84c5c92e_fk_perm",
		},
		{
			"22; 2606 63816822 TRIGGER public test_table test_table_update_trg user",
			TRIGGER,
			"test_table",
			"test_table_update_trg",
		},
		{
			"22; 2606 63816822 DEFAULT public test_table id user",
			DEFAULT,
			"test_table",
			"id",
		},
	}

	for _, tc := range testCases {
		meta, err := EntityMetaFromLine(tc.line)
		assert.Nil(t, err)
		assert.Equal(t, meta.DumpId, 22)
		assert.Equal(t, meta.TableOID, 2606)
		assert.Equal(t, meta.OID, 63816822)
		assert.Equal(t, meta.Desc, tc.desc)
		assert.Equal(t, meta.Schema, "public")
		assert.Equal(t, meta.Table, tc.table)
		assert.Equal(t, meta.Name, tc.name)
		assert.Equal(t, meta.Owner, "user")
	}
}

func TestEntityMetaCommonFromLine(t *testing.T) {
	testCases := []struct {
		line string
		desc EntityDescType
		name string
	}{
		{"1; 1 63658230 INDEX public test_table_1eba186c user", INDEX, "test_table_1eba186c"},
		{"1; 1 63658230 SEQUENCE public test_table_id_seq user", SEQUENCE, "test_table_id_seq"},
		{
			"1; 1 63658230 SEQUENCE OWNED BY public test_table_id_seq user",
			SEQUENCE_OWNED_BY,
			"test_table_id_seq",
		},
		{
			"1; 1 63658230 SEQUENCE SET public test_table_id_seq user",
			SEQUENCE_SET,
			"test_table_id_seq",
		},
		{"1; 1 63658230 TABLE public test_table user", TABLE, "test_table"},
		{"1; 1 63658230 TABLE DATA public test_table user", TABLE_DATA, "test_table"},
	}

	for _, tc := range testCases {
		meta, err := EntityMetaFromLine(tc.line)
		assert.Nil(t, err)
		assert.Equal(t, meta.DumpId, 1)
		assert.Equal(t, meta.TableOID, 1)
		assert.Equal(t, meta.OID, 63658230)
		assert.Equal(t, meta.Desc, tc.desc)
		assert.Equal(t, meta.Schema, "public")
		assert.Equal(t, meta.Table, "")
		assert.Equal(t, meta.Name, tc.name)
		assert.Equal(t, meta.Owner, "user")
	}
}

func TestEntityMetaFunctionFromLine(t *testing.T) {
	testCases := []struct {
		line string
		desc EntityDescType
		name string
	}{
		{
			"1; 1 63658230 FUNCTION public test_func(jsonb, jsonb) user",
			FUNCTION,
			"test_func(jsonb, jsonb)",
		},
		{"1; 1 63658230 FUNCTION public test_func() user", FUNCTION, "test_func()"},
	}

	for _, tc := range testCases {
		meta, err := EntityMetaFromLine(tc.line)
		assert.Nil(t, err)
		assert.Equal(t, meta.DumpId, 1)
		assert.Equal(t, meta.TableOID, 1)
		assert.Equal(t, meta.OID, 63658230)
		assert.Equal(t, meta.Desc, tc.desc)
		assert.Equal(t, meta.Schema, "public")
		assert.Equal(t, meta.Table, "")
		assert.Equal(t, meta.Name, tc.name)
		assert.Equal(t, meta.Owner, "user")
	}
}

func TestEntityMetaShortFromLine(t *testing.T) {
	testCases := []struct {
		line string
		desc EntityDescType
		name string
	}{
		{"1; 0 0 COMMENT - EXTENSION pg_stat_statements", COMMENT, "EXTENSION pg_stat_statements"},
		{"1; 0 0 EXTENSION - pg_stat_statements", EXTENSION, "pg_stat_statements"},
	}

	for _, tc := range testCases {
		meta, err := EntityMetaFromLine(tc.line)
		assert.Nil(t, err)
		assert.Equal(t, meta.DumpId, 1)
		assert.Equal(t, meta.TableOID, 0)
		assert.Equal(t, meta.OID, 0)
		assert.Equal(t, meta.Desc, tc.desc)
		assert.Equal(t, meta.Schema, "")
		assert.Equal(t, meta.Table, "")
		assert.Equal(t, meta.Name, tc.name)
		assert.Equal(t, meta.Owner, "")
	}
}
