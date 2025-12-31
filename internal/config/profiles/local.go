package profiles

import "github.com/danieljhkim/local-data-platform/internal/config/schema"

// LocalProfile returns the local profile configuration
// Local mode: Hive + Spark only (no HDFS/YARN, uses local filesystem)
func LocalProfile() *Profile {
	return &Profile{
		Name:        "local",
		Description: "Local mode: Hive + Spark only (no HDFS/YARN, uses local filesystem)",
		ConfigSet: &schema.ConfigSet{
			Hadoop: nil, // No Hadoop config for local profile
			Hive: &schema.HiveConfig{
				ConnectionURL:        "jdbc:postgresql://localhost:5432/metastore",
				ConnectionDriverName: "org.postgresql.Driver",
				ConnectionUserName:   "{{USER}}",
				ConnectionPassword:   "password",
				WarehouseDir:         "file:{{BASE_DIR}}/state/hive/warehouse", // Local filesystem
				TransportMode:        "binary",
				ThriftPort:           10000,
				Authentication:       "NONE",
				EnableDoAs:           false,
			},
			Spark: &schema.SparkConfig{
				Master:                "local[*]",
				AppName:               "local-data-platform-local",
				DriverMemory:          "5g",
				DriverMaxResultSize:   "2g",
				CatalogImplementation: "hive",
				WarehouseDir:          "file:{{BASE_DIR}}/state/hive/warehouse",
				EventLogEnabled:       false,
				ShufflePartitions:     8,
				AdaptiveEnabled:       true,
				Serializer:            "org.apache.spark.serializer.KryoSerializer",
			},
		},
	}
}
