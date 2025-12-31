package schema

import "strconv"

// SparkConfig represents spark-defaults.conf properties
type SparkConfig struct {
	// Execution
	Master     string // spark.master
	DeployMode string // spark.submit.deployMode
	AppName    string // spark.app.name

	// Resources
	DriverMemory        string // spark.driver.memory
	DriverMaxResultSize string // spark.driver.maxResultSize

	// Hadoop/HDFS integration
	HadoopDefaultFS string // spark.hadoop.fs.defaultFS

	// Hive integration
	CatalogImplementation string // spark.sql.catalogImplementation
	WarehouseDir          string // spark.sql.warehouse.dir (templated)

	// Event logging
	EventLogEnabled bool   // spark.eventLog.enabled
	EventLogDir     string // spark.eventLog.dir

	// SQL defaults
	ShufflePartitions  int    // spark.sql.shuffle.partitions
	AdaptiveEnabled    bool   // spark.sql.adaptive.enabled
	ParquetCompression string // spark.sql.parquet.compression.codec

	// Serialization
	Serializer string // spark.serializer

	// Compression
	IOCompressionCodec string // spark.io.compression.codec

	Extra []Property
}

// Clone creates a deep copy
func (c *SparkConfig) Clone() *SparkConfig {
	if c == nil {
		return nil
	}
	clone := *c
	clone.Extra = append([]Property{}, c.Extra...)
	return &clone
}

// ToProperties converts config to a list of properties with template substitution
func (c *SparkConfig) ToProperties(ctx *TemplateContext) []Property {
	var props []Property

	// Core execution settings
	if c.Master != "" {
		props = append(props, Property{Name: "spark.master", Value: c.Master})
	}
	if c.DeployMode != "" {
		props = append(props, Property{Name: "spark.submit.deployMode", Value: c.DeployMode})
	}
	if c.AppName != "" {
		props = append(props, Property{Name: "spark.app.name", Value: c.AppName})
	}

	// Resources
	if c.DriverMemory != "" {
		props = append(props, Property{Name: "spark.driver.memory", Value: c.DriverMemory})
	}
	if c.DriverMaxResultSize != "" {
		props = append(props, Property{Name: "spark.driver.maxResultSize", Value: c.DriverMaxResultSize})
	}

	// Hadoop integration
	if c.HadoopDefaultFS != "" {
		props = append(props, Property{Name: "spark.hadoop.fs.defaultFS", Value: c.HadoopDefaultFS})
	}

	// Hive integration
	if c.CatalogImplementation != "" {
		props = append(props, Property{Name: "spark.sql.catalogImplementation", Value: c.CatalogImplementation})
	}
	if c.WarehouseDir != "" {
		props = append(props, Property{Name: "spark.sql.warehouse.dir", Value: ctx.Substitute(c.WarehouseDir)})
	}

	// Event logging
	props = append(props, Property{Name: "spark.eventLog.enabled", Value: boolToString(c.EventLogEnabled)})
	if c.EventLogEnabled && c.EventLogDir != "" {
		props = append(props, Property{Name: "spark.eventLog.dir", Value: ctx.Substitute(c.EventLogDir)})
	}

	// SQL settings
	if c.ShufflePartitions > 0 {
		props = append(props, Property{Name: "spark.sql.shuffle.partitions", Value: strconv.Itoa(c.ShufflePartitions)})
	}
	props = append(props, Property{Name: "spark.sql.adaptive.enabled", Value: boolToString(c.AdaptiveEnabled)})
	if c.ParquetCompression != "" {
		props = append(props, Property{Name: "spark.sql.parquet.compression.codec", Value: c.ParquetCompression})
	}

	// Serialization
	if c.Serializer != "" {
		props = append(props, Property{Name: "spark.serializer", Value: c.Serializer})
	}

	// Compression
	if c.IOCompressionCodec != "" {
		props = append(props, Property{Name: "spark.io.compression.codec", Value: c.IOCompressionCodec})
	}

	return appendExtraProperties(props, c.Extra, ctx)
}
