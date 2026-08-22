package util

import (
	"strings"
	"testing"
)

func TestRedactJDBCURL(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "postgres userinfo",
			in:   "jdbc:postgresql://alice:s3cret-value@localhost:5432/metastore",
			want: "jdbc:postgresql://alice:********@localhost:5432/metastore",
		},
		{
			name: "mysql userinfo",
			in:   "jdbc:mysql://alice:s3cret-value@127.0.0.1:3306/metastore",
			want: "jdbc:mysql://alice:********@127.0.0.1:3306/metastore",
		},
		{
			name: "query password",
			in:   "jdbc:postgresql://localhost:5432/metastore?user=alice&password=s3cret-value",
			want: "jdbc:postgresql://localhost:5432/metastore?user=alice&password=********",
		},
		{
			name: "mysql pwd query",
			in:   "jdbc:mysql://localhost:3306/metastore?user=alice&pwd=s3cret-value",
			want: "jdbc:mysql://localhost:3306/metastore?user=alice&pwd=********",
		},
		{
			name: "no secret unchanged",
			in:   "jdbc:postgresql://localhost:5432/metastore",
			want: "jdbc:postgresql://localhost:5432/metastore",
		},
		{
			name: "derby unchanged",
			in:   "jdbc:derby:;databaseName=/tmp/metastore_db;create=true",
			want: "jdbc:derby:;databaseName=/tmp/metastore_db;create=true",
		},
		{
			name: "empty",
			in:   "",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RedactJDBCURL(tt.in)
			if got != tt.want {
				t.Fatalf("RedactJDBCURL(%q) = %q, want %q", tt.in, got, tt.want)
			}
			if strings.Contains(got, "s3cret-value") {
				t.Fatalf("redacted URL leaked secret: %q", got)
			}
		})
	}
}
