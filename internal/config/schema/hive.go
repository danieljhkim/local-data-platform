package schema

import "strconv"

// HiveConfig represents hive-site.xml properties
type HiveConfig struct {
	// Metastore database connection
	ConnectionURL        string // javax.jdo.option.ConnectionURL
	ConnectionDriverName string // javax.jdo.option.ConnectionDriverName
	ConnectionUserName   string // javax.jdo.option.ConnectionUserName (templated)
	ConnectionPassword   string // javax.jdo.option.ConnectionPassword
	MetastoreURIs        string // hive.metastore.uris (for HS2 -> metastore service)
	// MetastoreThriftBindHost is hive.metastore.thrift.bind.host, the Hive
	// alias of MetastoreConf.THRIFT_BIND_HOST. Hive 4.2.1 defaults that key
	// and metastore.thrift.bind.host to empty. Shipped defaults leave the
	// primary key unset, so MetastoreConf.getVar reads this alias. An empty
	// host makes SecurityUtils.getServerSocket build InetSocketAddress(port),
	// a wildcard socket. hive.metastore.uris is only the client endpoint.
	// The value is generated configuration, not a live listener observation.
	MetastoreThriftBindHost string

	// Warehouse
	WarehouseDir string // hive.metastore.warehouse.dir

	// HiveServer2
	TransportMode string // hive.server2.transport.mode
	ThriftPort    int    // hive.server2.thrift.port
	// ThriftBindHost is hive.server2.thrift.bind.host. Hive 4.2.1
	// HiveConf defaults it to empty, and HiveAuthUtils.getServerSocket
	// wildcard-binds an empty host. Profiles set an explicit loopback host.
	ThriftBindHost string
	Authentication string // hive.server2.authentication
	EnableDoAs     bool   // hive.server2.enable.doAs

	// HiveServer2 Web UI
	WebUIHost string // hive.server2.webui.host
	WebUIPort int    // hive.server2.webui.port (0 disables the Web UI)

	// Schema verification
	SchemaVerification bool // hive.metastore.schema.verification
	AutoCreateSchema   bool // datanucleus.schema.autoCreateAll

	Extra []Property
}

// Clone creates a deep copy
func (c *HiveConfig) Clone() *HiveConfig {
	if c == nil {
		return nil
	}
	clone := *c
	clone.Extra = append([]Property{}, c.Extra...)
	return &clone
}

// ToProperties converts config to a list of properties with template substitution
func (c *HiveConfig) ToProperties(ctx *TemplateContext) []Property {
	props := []Property{
		// Metastore connection
		{Name: "javax.jdo.option.ConnectionURL", Value: c.ConnectionURL},
		{Name: "javax.jdo.option.ConnectionDriverName", Value: c.ConnectionDriverName},
		{Name: "javax.jdo.option.ConnectionUserName", Value: ctx.Substitute(c.ConnectionUserName)},
		{Name: "javax.jdo.option.ConnectionPassword", Value: c.ConnectionPassword},
		{Name: "hive.metastore.uris", Value: c.MetastoreURIs},
		{Name: "hive.metastore.thrift.bind.host", Value: c.MetastoreThriftBindHost},

		// Warehouse
		{Name: "hive.metastore.warehouse.dir", Value: ctx.Substitute(c.WarehouseDir)},

		// HiveServer2
		{Name: "hive.server2.transport.mode", Value: c.TransportMode},
		{Name: "hive.server2.thrift.port", Value: strconv.Itoa(c.ThriftPort)},
		{Name: "hive.server2.thrift.bind.host", Value: c.ThriftBindHost},
		{Name: "hive.server2.authentication", Value: c.Authentication},
		{Name: "hive.server2.enable.doAs", Value: boolToString(c.EnableDoAs)},

		// HiveServer2 Web UI
		{Name: "hive.server2.webui.host", Value: c.WebUIHost},
		{Name: "hive.server2.webui.port", Value: strconv.Itoa(c.WebUIPort)},

		// Schema
		{Name: "hive.metastore.schema.verification", Value: boolToString(c.SchemaVerification)},
		{Name: "datanucleus.schema.autoCreateAll", Value: boolToString(c.AutoCreateSchema)},
	}
	return applyExtraOverrides(props, c.Extra, ctx)
}

// applyExtraOverrides appends user properties. A name the typed config already
// emitted is replaced in place so overrides.yaml is the only value of that
// property. Hive reads Hadoop configuration, which keeps the last duplicate,
// while this repository's XML reader keeps the first. One property makes
// both readers honor a deliberate override.
func applyExtraOverrides(props []Property, extra []Property, ctx *TemplateContext) []Property {
	index := make(map[string]int, len(props))
	for i, prop := range props {
		index[prop.Name] = i
	}
	for _, prop := range extra {
		replaced := Property{Name: prop.Name, Value: ctx.Substitute(prop.Value)}
		if i, ok := index[prop.Name]; ok {
			props[i] = replaced
			continue
		}
		index[prop.Name] = len(props)
		props = append(props, replaced)
	}
	return props
}
