package profiles

import "github.com/danieljhkim/local-data-platform/internal/config/schema"

// HDFSProfile returns the HDFS profile configuration
// Full Hadoop stack: HDFS + YARN + Hive + Spark (uses HDFS for storage)
func HDFSProfile() *Profile {
	return &Profile{
		Name:        "hdfs",
		Description: "Full Hadoop stack: HDFS + YARN + Hive + Spark (uses HDFS for storage)",
		ConfigSet: &schema.ConfigSet{
			Hadoop: &schema.HadoopConfig{
				CoreSite: &schema.CoreSiteConfig{
					DefaultFS:              "hdfs://localhost:8020",
					TmpDir:                 "{{BASE_DIR}}/state/hadoop/tmp",
					SecurityAuthentication: "simple",
					SecurityAuthorization:  false,
					FallbackToSimpleAuth:   true,
				},
				HDFSSite: &schema.HDFSSiteConfig{
					Replication:        1,
					NameNodeRPCAddress: "localhost:8020",
					NameNodeNameDir:    "file:{{BASE_DIR}}/state/hdfs/namenode",
					DataNodeDataDir:    "file:{{BASE_DIR}}/state/hdfs/datanode",
				},
				YarnSite: &schema.YarnSiteConfig{
					AuxServices:             "mapreduce_shuffle",
					AuxServicesClass:        "org.apache.hadoop.mapred.ShuffleHandler",
					ResourceManagerHostname: "localhost",
					NodeManagerHostname:     "localhost",
					NodeManagerBindHost:     "127.0.0.1",
					NodeManagerAddress:      "127.0.0.1:0",
					LocalizerAddress:        "127.0.0.1:8040",
					WebAppAddress:           "127.0.0.1:8042",
					ContainerExecutorClass:  "org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor",
					ShuffleSSLEnabled:       false,
					MemoryMB:                8192,
					VCores:                  4,
					VMemCheckEnabled:        false,
					PMemCheckEnabled:        false,
				},
				MapredSite: &schema.MapredSiteConfig{
					FrameworkName:        "yarn",
					ApplicationClasspath: "$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*,$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*",
				},
				CapacityScheduler: &schema.CapacitySchedulerConfig{
					RootQueues:         "default",
					DefaultCapacity:    100,
					DefaultMaxCapacity: 100,
					DefaultState:       "RUNNING",
				},
			},
			Hive: &schema.HiveConfig{
				ConnectionURL:        "jdbc:postgresql://localhost:5432/metastore",
				ConnectionDriverName: "org.postgresql.Driver",
				ConnectionUserName:   "{{USER}}",
				ConnectionPassword:   "password",
				WarehouseDir:         "/user/hive/warehouse", // HDFS path
				TransportMode:        "binary",
				ThriftPort:           10000,
				Authentication:       "NONE",
				EnableDoAs:           false,
			},
			Spark: &schema.SparkConfig{
				Master:                "local[*]",
				DeployMode:            "client",
				AppName:               "local-data-platform-hdfs",
				DriverMemory:          "5g",
				HadoopDefaultFS:       "hdfs://localhost:8020",
				CatalogImplementation: "hive",
				WarehouseDir:          "/user/hive/warehouse",
				EventLogEnabled:       true,
				EventLogDir:           "hdfs:///spark-history",
				ShufflePartitions:     8,
				AdaptiveEnabled:       true,
				ParquetCompression:    "snappy",
				Serializer:            "org.apache.spark.serializer.KryoSerializer",
			},
		},
	}
}
