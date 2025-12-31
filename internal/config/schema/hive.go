package schema

import "strconv"

// HiveConfig represents hive-site.xml properties
type HiveConfig struct {
	// Metastore database connection
	ConnectionURL        string // javax.jdo.option.ConnectionURL
	ConnectionDriverName string // javax.jdo.option.ConnectionDriverName
	ConnectionUserName   string // javax.jdo.option.ConnectionUserName (templated)
	ConnectionPassword   string // javax.jdo.option.ConnectionPassword

	// Warehouse
	WarehouseDir string // hive.metastore.warehouse.dir

	// HiveServer2
	TransportMode  string // hive.server2.transport.mode
	ThriftPort     int    // hive.server2.thrift.port
	Authentication string // hive.server2.authentication
	EnableDoAs     bool   // hive.server2.enable.doAs

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

		// Warehouse
		{Name: "hive.metastore.warehouse.dir", Value: ctx.Substitute(c.WarehouseDir)},

		// HiveServer2
		{Name: "hive.server2.transport.mode", Value: c.TransportMode},
		{Name: "hive.server2.thrift.port", Value: strconv.Itoa(c.ThriftPort)},
		{Name: "hive.server2.authentication", Value: c.Authentication},
		{Name: "hive.server2.enable.doAs", Value: boolToString(c.EnableDoAs)},

		// Schema
		{Name: "hive.metastore.schema.verification", Value: boolToString(c.SchemaVerification)},
		{Name: "datanucleus.schema.autoCreateAll", Value: boolToString(c.AutoCreateSchema)},
	}
	return appendExtraProperties(props, c.Extra, ctx)
}
