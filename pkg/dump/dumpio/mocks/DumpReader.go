// Code generated by mockery v2.51.0. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// DumpReader is an autogenerated mock type for the DumpReader type
type DumpReader struct {
	mock.Mock
}

// Close provides a mock function with no fields
func (_m *DumpReader) Close() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Close")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Open provides a mock function with no fields
func (_m *DumpReader) Open() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Open")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Read provides a mock function with given fields: p
func (_m *DumpReader) Read(p []byte) (int, error) {
	ret := _m.Called(p)

	if len(ret) == 0 {
		panic("no return value specified for Read")
	}

	var r0 int
	var r1 error
	if rf, ok := ret.Get(0).(func([]byte) (int, error)); ok {
		return rf(p)
	}
	if rf, ok := ret.Get(0).(func([]byte) int); ok {
		r0 = rf(p)
	} else {
		r0 = ret.Get(0).(int)
	}

	if rf, ok := ret.Get(1).(func([]byte) error); ok {
		r1 = rf(p)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewDumpReader creates a new instance of DumpReader. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewDumpReader(t interface {
	mock.TestingT
	Cleanup(func())
},
) *DumpReader {
	mock := &DumpReader{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
