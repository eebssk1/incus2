package usage

// LegacyKV is a backward-compatible key/value parsing atom.
var LegacyKV = hide{alternative{[]Atom{compound{"=", []Atom{Key, Value}}, compound{" ", []Atom{Key, Value}}}}, compound{"=", []Atom{Key, Value}}}
