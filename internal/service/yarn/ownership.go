package yarn

import "github.com/danieljhkim/local-data-platform/internal/service"

func yarnPIDValidator() service.PIDValidator {
	return service.NewProcessMatchValidator("YARN", map[string][]string{
		"resourcemanager": {
			"org.apache.hadoop.yarn.server.resourcemanager.ResourceManager",
			"ResourceManager",
			"yarn resourcemanager",
		},
		"nodemanager": {
			"org.apache.hadoop.yarn.server.nodemanager.NodeManager",
			"NodeManager",
			"yarn nodemanager",
		},
	})
}
