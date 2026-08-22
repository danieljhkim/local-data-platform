package metastore

import (
	"strings"
	"testing"
)

func TestValidateURL_RedactsPasswordInErrors(t *testing.T) {
	const secret = "s3cret-value"
	err := ValidateURL(Derby, "jdbc:postgresql://alice:"+secret+"@localhost:5432/metastore")
	if err == nil {
		t.Fatal("expected mismatch error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("ValidateURL error leaked secret: %v", err)
	}
	if !strings.Contains(err.Error(), "********") {
		t.Fatalf("expected redacted URL in error: %v", err)
	}
}

func TestValidateURL_UnsupportedRedactsPassword(t *testing.T) {
	const secret = "s3cret-value"
	err := ValidateURL(Postgres, "not-a-jdbc://alice:"+secret+"@localhost/db")
	if err == nil {
		t.Fatal("expected unsupported URL error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("ValidateURL error leaked secret: %v", err)
	}
}
