package main

import (
	"net"
	"strconv"
	"testing"
)

func TestRunHealthcheck(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	_, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	if code := runHealthcheck(port); code != 0 {
		t.Fatalf("healthcheck code = %d", code)
	}
	if code := runHealthcheck(1); code == 0 {
		t.Fatal("closed port reported healthy")
	}
}
