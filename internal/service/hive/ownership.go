package hive

import "github.com/danieljhkim/local-data-platform/internal/service"

func hivePIDValidator() service.PIDValidator {
	return service.NewProcessMatchValidator("Hive", map[string][]string{
		"metastore": {
			"HiveMetaStore",
			"org.apache.hadoop.hive.metastore",
			"hive --service metastore",
		},
		"hiveserver2": {
			"HiveServer2",
			"hiveserver2",
			"org.apache.hive.service.server.HiveServer2",
			"hive --service hiveserver2",
		},
	})
}
