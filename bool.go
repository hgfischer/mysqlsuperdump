package main

import (
	"fmt"
	"os"
)

type Bool bool

func (b Bool) Printf(s string, a ...interface{}) {
	if b {
		fmt.Fprintf(os.Stdout, s, a...)
	}
}
