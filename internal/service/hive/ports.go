package hive

import (
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/danieljhkim/local-data-platform/internal/util"
)

const (
	defaultMetastorePort   = 9083
	defaultHiveServer2Port = 10000
)

type hiveListenerPorts struct {
	Metastore   int
	HiveServer2 int
}

func defaultHivePorts() hiveListenerPorts {
	return hiveListenerPorts{
		Metastore:   defaultMetastorePort,
		HiveServer2: defaultHiveServer2Port,
	}
}

func (p hiveListenerPorts) slice() []int {
	return []int{p.Metastore, p.HiveServer2}
}

func (h *HiveService) listenerPorts() hiveListenerPorts {
	if h == nil || h.env == nil || h.env.HiveConfDir == "" {
		return defaultHivePorts()
	}
	return readHiveListenerPorts(filepath.Join(h.env.HiveConfDir, "hive-site.xml"))
}

func readHiveListenerPorts(hiveSite string) hiveListenerPorts {
	ports := defaultHivePorts()

	cfg, err := util.ParseHadoopXML(hiveSite)
	if err != nil {
		return ports
	}

	if raw := strings.TrimSpace(cfg.GetProperty("hive.metastore.uris")); raw != "" {
		port, err := parseMetastorePort(raw)
		if err != nil {
			util.Warn("Invalid hive.metastore.uris value %q; using default port %d.", raw, defaultMetastorePort)
		} else {
			ports.Metastore = port
		}
	}

	if raw := strings.TrimSpace(cfg.GetProperty("hive.server2.thrift.port")); raw != "" {
		port, err := parsePort(raw)
		if err != nil {
			util.Warn("Invalid hive.server2.thrift.port value %q; using default port %d.", raw, defaultHiveServer2Port)
		} else {
			ports.HiveServer2 = port
		}
	}

	return ports
}

func parseMetastorePort(raw string) (int, error) {
	var firstErr error
	for _, candidate := range strings.Split(raw, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}

		portStr, err := extractPort(candidate)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		port, err := parsePort(portStr)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		return port, nil
	}

	if firstErr != nil {
		return 0, firstErr
	}
	return 0, fmt.Errorf("no metastore URI port configured")
}

func extractPort(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err == nil {
		if port := parsed.Port(); port != "" {
			return port, nil
		}
		if parsed.Host != "" {
			return "", fmt.Errorf("missing port in %q", raw)
		}
	}

	if host, port, splitErr := net.SplitHostPort(raw); splitErr == nil && host != "" {
		return port, nil
	}

	if idx := strings.LastIndex(raw, ":"); idx >= 0 && idx < len(raw)-1 {
		return raw[idx+1:], nil
	}

	if err != nil {
		return "", err
	}
	return "", fmt.Errorf("missing port in %q", raw)
}

func parsePort(raw string) (int, error) {
	port, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, err
	}
	if port <= 0 || port > 65535 {
		return 0, fmt.Errorf("port %d is outside valid range", port)
	}
	return port, nil
}
