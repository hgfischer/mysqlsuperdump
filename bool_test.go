package main

import (
	"os"
	"testing"
)

type IOWriterMock struct {
	WriteMock func(p []byte)
}

func (m *IOWriterMock) Write(p []byte) (n int, err error) {
	m.WriteMock(p)
	return len(p), nil
}

func TestTrueBool(t *testing.T) {
	trueBool := Bool(true)
	expected := "test"
	called := false
	BoolWriter = &IOWriterMock{
		WriteMock: func(p []byte) {
			called = true
			if string(p) != expected {
				t.Errorf("Got %+v instead of %+v", p, expected)
			}
		},
	}
	if !called {
		t.Errorf("Should have printed the expected string with a true Bool")
	}
	trueBool.Printf(expected)
	BoolWriter = os.Stdout
}

func TestFalseBool(t *testing.T) {
	falseBool := Bool(false)
	called := false
	BoolWriter = &IOWriterMock{
		WriteMock: func(p []byte) {
			called = true
		},
	}
	falseBool.Printf("test")
	if called {
		t.Errorf("Nothing should be printed with a false Bool")
	}
	BoolWriter = os.Stdout
}