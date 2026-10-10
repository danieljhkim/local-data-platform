package generator

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config/profiles"
)

// Hadoop 3.5.0 defaults dfs.datanode.address to 0.0.0.0:9866 and
// dfs.datanode.ipc.address to 0.0.0.0:9867. These tests read generated
// hdfs-site.xml only. They do not start a DataNode and do not observe a
// listening socket.

const (
	dataNodeTransferAddress = "127.0.0.1:9866"
	dataNodeIPCAddress      = "127.0.0.1:9867"
	nameNodeRPCAddress      = "localhost:8020"
	nameNodeHTTPAddress     = "127.0.0.1:9870"
	dataNodeHTTPAddress     = "127.0.0.1:9864"
)

func TestGeneratedHDFSProfile_DataNodeProtocolsBindToLoopback(t *testing.T) {
	reg := profiles.NewRegistry()
	gen := NewConfigGenerator()
	if !reg.Has("hdfs") {
		t.Fatal("built-in hdfs profile is missing")
	}
	if !reg.Has("local") {
		t.Fatal("built-in local profile is missing")
	}

	t.Run("hdfs", func(t *testing.T) {
		baseDir := t.TempDir()
		destDir := t.TempDir()
		if err := gen.Generate("hdfs", baseDir, destDir); err != nil {
			t.Fatalf("Generate(hdfs) failed: %v", err)
		}

		hdfsSite := filepath.Join(destDir, "hadoop", "hdfs-site.xml")
		assertLoopbackEndpoint(t, hdfsSite, "dfs.datanode.address", dataNodeTransferAddress)
		assertLoopbackEndpoint(t, hdfsSite, "dfs.datanode.ipc.address", dataNodeIPCAddress)
		assertSingleProperty(t, hdfsSite, "dfs.namenode.rpc-address", nameNodeRPCAddress)
		assertSingleProperty(t, hdfsSite, "dfs.namenode.http-address", nameNodeHTTPAddress)
		assertSingleProperty(t, hdfsSite, "dfs.datanode.http.address", dataNodeHTTPAddress)
	})

	t.Run("local", func(t *testing.T) {
		baseDir := t.TempDir()
		destDir := t.TempDir()
		if err := gen.Generate("local", baseDir, destDir); err != nil {
			t.Fatalf("Generate(local) failed: %v", err)
		}

		hdfsSite := filepath.Join(destDir, "hadoop", "hdfs-site.xml")
		if _, err := os.Stat(hdfsSite); !os.IsNotExist(err) {
			t.Fatalf("local profile emitted hdfs-site.xml (stat err=%v); it has no Hadoop config", err)
		}
	})
}

func assertLoopbackEndpoint(t *testing.T, path, name, want string) {
	t.Helper()

	values := hadoopPropertyValues(t, path, name)
	if len(values) != 1 || values[0] != want {
		t.Errorf("%s: property %q = %q, want exactly [%q]", filepath.Base(path), name, values, want)
		return
	}

	host, port, err := net.SplitHostPort(values[0])
	if err != nil {
		t.Errorf("%s: property %q value %q is not host:port: %v", filepath.Base(path), name, values[0], err)
		return
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		t.Errorf("%s: property %q host %q is not a loopback address", filepath.Base(path), name, host)
	}
	if port == "" {
		t.Errorf("%s: property %q has an empty port", filepath.Base(path), name)
	}
}
