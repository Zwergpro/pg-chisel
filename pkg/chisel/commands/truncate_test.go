package commands

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zwergpro/pg-chisel/pkg/dump"
	"github.com/zwergpro/pg-chisel/pkg/dump/dumpio/mocks"
)

func TestTruncateCmd_Execute(t *testing.T) {
	t.Run("successful execution", func(t *testing.T) {
		writer := mocks.NewDumpWriter(t)
		writer.On("Open").Return(nil)
		writer.On("Write", []byte("\\.\n\n")).Return(len("\\.\n\n"), nil)
		writer.On("Close").Return(nil)

		handler := mocks.NewDumpHandler(t)
		handler.On("GetWriter").Return(writer)

		cmd := NewTruncateCmd(&dump.Entity{}, handler)

		err := cmd.Execute()
		require.NoError(t, err)

		writer.AssertExpectations(t)
		handler.AssertExpectations(t)
	})

	t.Run("writer open failure", func(t *testing.T) {
		writer := mocks.NewDumpWriter(t)
		writer.On("Open").Return(errors.New("open error"))

		handler := mocks.NewDumpHandler(t)
		handler.On("GetWriter").Return(writer)

		cmd := NewTruncateCmd(&dump.Entity{}, handler)

		err := cmd.Execute()
		require.Error(t, err)
		require.Contains(t, err.Error(), "open error")

		writer.AssertExpectations(t)
		handler.AssertExpectations(t)
	})

	t.Run("write failure", func(t *testing.T) {
		writer := mocks.NewDumpWriter(t)
		writer.On("Open").Return(nil)
		writer.On("Write", []byte("\\.\n\n")).Return(0, errors.New("write error"))
		writer.On("Close").Return(nil)

		handler := mocks.NewDumpHandler(t)
		handler.On("GetWriter").Return(writer)

		cmd := NewTruncateCmd(&dump.Entity{}, handler)

		err := cmd.Execute()
		require.Error(t, err)
		require.Contains(t, err.Error(), "write error")

		writer.AssertExpectations(t)
		handler.AssertExpectations(t)
	})

	t.Run("close failure", func(t *testing.T) {
		writer := mocks.NewDumpWriter(t)
		writer.On("Open").Return(nil)
		writer.On("Write", []byte("\\.\n\n")).Return(len("\\.\n\n"), nil)
		writer.On("Close").Return(errors.New("close error"))

		handler := mocks.NewDumpHandler(t)
		handler.On("GetWriter").Return(writer)

		cmd := NewTruncateCmd(&dump.Entity{}, handler)

		err := cmd.Execute()
		require.NoError(t, err, "close errors should not fail execution")

		writer.AssertExpectations(t)
		handler.AssertExpectations(t)
	})
}
