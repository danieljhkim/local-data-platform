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
				ConnectionURL:        "jdbc:derby:;databaseName=metastore_db;create=true",
				ConnectionDriverName: "org.apache.derby.iapi.jdbc.AutoloadedDriver",
				ConnectionUserName:   "APP",
				ConnectionPassword:   "password",
				MetastoreURIs:        "thrift://localhost:9083",
				WarehouseDir:         "file:{{BASE_DIR}}/state/hive/warehouse", // Local filesystem
				TransportMode:        "binary",
				ThriftPort:           10000,
				Authentication:       "NONE",
				EnableDoAs:           false,
				Extra: []schema.Property{
					{Name: "hive.metastore.event.db.notification.api.auth", Value: "false"},
					{Name: "hive.execution.engine", Value: "mr"},
					{Name: "hive.server2.tez.initialize.default.sessions", Value: "false"},
				},
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
