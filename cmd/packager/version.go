package main

// Version is injected at build time via -ldflags "-X main.Version=x.y.z".
// Falls back to "dev" for local builds without the flag.
var Version = "dev"
var Version2 = "1.2.22"
