package main

import (
	"fmt"
	"io"
	"os"
)

type Bool bool

var BoolWriter io.Writer = os.Stdout

func (b Bool) Printf(s string, a ...interface{}) {
	if b {
		fmt.Fprintf(BoolWriter, s, a...)
	}
}
