package cmd

import (
	"strings"

	"github.com/spf13/pflag"
)

// AddStringFlag adds a string flag to the given flag set.
func AddStringFlag(flags *pflag.FlagSet, flag *string, name string, defVal string, noOptDefVal string, usage string) {
	name, shorthand, _ := strings.Cut(name, "|")
	// Cobra handles value hints and backticks in a way that doesn’t suit us. Prepending two
	// backticks is a way to solve that.
	flags.StringVarP(flag, name, shorthand, defVal, "``"+usage)
	flags.Lookup(name).NoOptDefVal = noOptDefVal
}
