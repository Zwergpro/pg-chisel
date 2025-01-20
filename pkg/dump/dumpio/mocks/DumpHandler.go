// Code generated by mockery v2.51.0. DO NOT EDIT.

package mocks

import (
	mock "github.com/stretchr/testify/mock"
	dumpio "github.com/zwergpro/pg-chisel/pkg/dump/dumpio"
)

// DumpHandler is an autogenerated mock type for the DumpHandler type
type DumpHandler struct {
	mock.Mock
}

// GetReader provides a mock function with no fields
func (_m *DumpHandler) GetReader() dumpio.DumpReader {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetReader")
	}

	var r0 dumpio.DumpReader
	if rf, ok := ret.Get(0).(func() dumpio.DumpReader); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(dumpio.DumpReader)
		}
	}

	return r0
}

// GetWriter provides a mock function with no fields
func (_m *DumpHandler) GetWriter() dumpio.DumpWriter {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetWriter")
	}

	var r0 dumpio.DumpWriter
	if rf, ok := ret.Get(0).(func() dumpio.DumpWriter); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(dumpio.DumpWriter)
		}
	}

	return r0
}

// NewDumpHandler creates a new instance of DumpHandler. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewDumpHandler(t interface {
	mock.TestingT
	Cleanup(func())
},
) *DumpHandler {
	mock := &DumpHandler{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
