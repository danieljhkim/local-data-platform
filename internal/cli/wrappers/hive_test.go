package wrappers

import (
	"reflect"
	"testing"
)

func TestBeelineArgs_UsesConfiguredDefaultURL(t *testing.T) {
	got := beelineArgs("jdbc:hive2://localhost:11000", []string{"-e", "SELECT 1"})
	want := []string{"beeline", "-u", "jdbc:hive2://localhost:11000", "-e", "SELECT 1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("beelineArgs() = %#v, want %#v", got, want)
	}
}

func TestBeelineArgs_ExplicitURLTakesPrecedence(t *testing.T) {
	for _, args := range [][]string{
		{"-u", "jdbc:hive2://127.0.0.1:12000", "-e", "SELECT 1"},
		{"--url=jdbc:hive2://127.0.0.1:12000", "-e", "SELECT 1"},
	} {
		got := beelineArgs("jdbc:hive2://localhost:11000", args)
		want := append([]string{"beeline"}, args...)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("beelineArgs(%#v) = %#v, want %#v", args, got, want)
		}
	}
}
