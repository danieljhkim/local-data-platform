package hdfs

import "github.com/danieljhkim/local-data-platform/internal/service"

func hdfsPIDValidator() service.PIDValidator {
	return service.NewProcessMatchValidator("HDFS", map[string][]string{
		"namenode": {
			"org.apache.hadoop.hdfs.server.namenode.NameNode",
			"NameNode",
			"hdfs namenode",
		},
		"datanode": {
			"org.apache.hadoop.hdfs.server.datanode.DataNode",
			"DataNode",
			"hdfs datanode",
		},
	})
}
