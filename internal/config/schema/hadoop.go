package schema

import "strconv"

// HadoopConfig represents all Hadoop configuration files
type HadoopConfig struct {
	CoreSite          *CoreSiteConfig
	HDFSSite          *HDFSSiteConfig
	YarnSite          *YarnSiteConfig
	MapredSite        *MapredSiteConfig
	CapacityScheduler *CapacitySchedulerConfig
}

// Clone creates a deep copy of HadoopConfig
func (c *HadoopConfig) Clone() *HadoopConfig {
	if c == nil {
		return nil
	}
	clone := &HadoopConfig{}
	if c.CoreSite != nil {
		clone.CoreSite = c.CoreSite.Clone()
	}
	if c.HDFSSite != nil {
		clone.HDFSSite = c.HDFSSite.Clone()
	}
	if c.YarnSite != nil {
		clone.YarnSite = c.YarnSite.Clone()
	}
	if c.MapredSite != nil {
		clone.MapredSite = c.MapredSite.Clone()
	}
	if c.CapacityScheduler != nil {
		clone.CapacityScheduler = c.CapacityScheduler.Clone()
	}
	return clone
}

// CoreSiteConfig represents core-site.xml properties
type CoreSiteConfig struct {
	DefaultFS              string // fs.defaultFS
	TmpDir                 string // hadoop.tmp.dir (templated)
	SecurityAuthentication string // hadoop.security.authentication
	SecurityAuthorization  bool   // hadoop.security.authorization
	FallbackToSimpleAuth   bool   // ipc.client.fallback-to-simple-auth-allowed
	Extra                  []Property
}

// Clone creates a deep copy
func (c *CoreSiteConfig) Clone() *CoreSiteConfig {
	if c == nil {
		return nil
	}
	clone := *c
	clone.Extra = append([]Property{}, c.Extra...)
	return &clone
}

// ToProperties converts config to a list of properties with template substitution
func (c *CoreSiteConfig) ToProperties(ctx *TemplateContext) []Property {
	props := []Property{
		{Name: "fs.defaultFS", Value: c.DefaultFS},
		{Name: "hadoop.tmp.dir", Value: ctx.Substitute(c.TmpDir)},
		{Name: "hadoop.security.authentication", Value: c.SecurityAuthentication},
		{Name: "hadoop.security.authorization", Value: boolToString(c.SecurityAuthorization)},
		{Name: "ipc.client.fallback-to-simple-auth-allowed", Value: boolToString(c.FallbackToSimpleAuth)},
	}
	return appendExtraProperties(props, c.Extra, ctx)
}

// HDFSSiteConfig represents hdfs-site.xml properties
type HDFSSiteConfig struct {
	Replication        int    // dfs.replication
	NameNodeRPCAddress string // dfs.namenode.rpc-address
	NameNodeNameDir    string // dfs.namenode.name.dir (templated)
	DataNodeDataDir    string // dfs.datanode.data.dir (templated)
	Extra              []Property
}

// Clone creates a deep copy
func (c *HDFSSiteConfig) Clone() *HDFSSiteConfig {
	if c == nil {
		return nil
	}
	clone := *c
	clone.Extra = append([]Property{}, c.Extra...)
	return &clone
}

// ToProperties converts config to a list of properties with template substitution
func (c *HDFSSiteConfig) ToProperties(ctx *TemplateContext) []Property {
	props := []Property{
		{Name: "dfs.replication", Value: strconv.Itoa(c.Replication)},
		{Name: "dfs.namenode.rpc-address", Value: c.NameNodeRPCAddress},
		{Name: "dfs.namenode.name.dir", Value: ctx.Substitute(c.NameNodeNameDir)},
		{Name: "dfs.datanode.data.dir", Value: ctx.Substitute(c.DataNodeDataDir)},
	}
	return appendExtraProperties(props, c.Extra, ctx)
}

// YarnSiteConfig represents yarn-site.xml properties
type YarnSiteConfig struct {
	AuxServices             string // yarn.nodemanager.aux-services
	AuxServicesClass        string // yarn.nodemanager.aux-services.mapreduce_shuffle.class
	ResourceManagerHostname string // yarn.resourcemanager.hostname
	NodeManagerHostname     string // yarn.nodemanager.hostname
	NodeManagerBindHost     string // yarn.nodemanager.bind-host
	NodeManagerAddress      string // yarn.nodemanager.address
	LocalizerAddress        string // yarn.nodemanager.localizer.address
	WebAppAddress           string // yarn.nodemanager.webapp.address
	ContainerExecutorClass  string // yarn.nodemanager.container-executor.class
	ShuffleSSLEnabled       bool   // mapreduce.shuffle.ssl.enabled
	MemoryMB                int    // yarn.nodemanager.resource.memory-mb
	VCores                  int    // yarn.nodemanager.resource.cpu-vcores
	VMemCheckEnabled        bool   // yarn.nodemanager.vmem-check-enabled
	PMemCheckEnabled        bool   // yarn.nodemanager.pmem-check-enabled
	Extra                   []Property
}

// Clone creates a deep copy
func (c *YarnSiteConfig) Clone() *YarnSiteConfig {
	if c == nil {
		return nil
	}
	clone := *c
	clone.Extra = append([]Property{}, c.Extra...)
	return &clone
}

// ToProperties converts config to a list of properties
func (c *YarnSiteConfig) ToProperties(ctx *TemplateContext) []Property {
	props := []Property{
		{Name: "yarn.nodemanager.aux-services", Value: c.AuxServices},
		{Name: "yarn.nodemanager.aux-services.mapreduce_shuffle.class", Value: c.AuxServicesClass},
		{Name: "yarn.resourcemanager.hostname", Value: c.ResourceManagerHostname},
		{Name: "yarn.nodemanager.hostname", Value: c.NodeManagerHostname},
		{Name: "yarn.nodemanager.bind-host", Value: c.NodeManagerBindHost},
		{Name: "yarn.nodemanager.address", Value: c.NodeManagerAddress},
		{Name: "yarn.nodemanager.localizer.address", Value: c.LocalizerAddress},
		{Name: "yarn.nodemanager.webapp.address", Value: c.WebAppAddress},
		{Name: "yarn.nodemanager.container-executor.class", Value: c.ContainerExecutorClass},
		{Name: "mapreduce.shuffle.ssl.enabled", Value: boolToString(c.ShuffleSSLEnabled)},
		{Name: "yarn.nodemanager.resource.memory-mb", Value: strconv.Itoa(c.MemoryMB)},
		{Name: "yarn.nodemanager.resource.cpu-vcores", Value: strconv.Itoa(c.VCores)},
		{Name: "yarn.nodemanager.vmem-check-enabled", Value: boolToString(c.VMemCheckEnabled)},
		{Name: "yarn.nodemanager.pmem-check-enabled", Value: boolToString(c.PMemCheckEnabled)},
	}
	return appendExtraProperties(props, c.Extra, ctx)
}

// MapredSiteConfig represents mapred-site.xml properties
type MapredSiteConfig struct {
	FrameworkName        string // mapreduce.framework.name
	ApplicationClasspath string // mapreduce.application.classpath
	Extra                []Property
}

// Clone creates a deep copy
func (c *MapredSiteConfig) Clone() *MapredSiteConfig {
	if c == nil {
		return nil
	}
	clone := *c
	clone.Extra = append([]Property{}, c.Extra...)
	return &clone
}

// ToProperties converts config to a list of properties
func (c *MapredSiteConfig) ToProperties(ctx *TemplateContext) []Property {
	props := []Property{
		{Name: "mapreduce.framework.name", Value: c.FrameworkName},
		{Name: "mapreduce.application.classpath", Value: c.ApplicationClasspath},
	}
	return appendExtraProperties(props, c.Extra, ctx)
}

// CapacitySchedulerConfig represents capacity-scheduler.xml properties
type CapacitySchedulerConfig struct {
	RootQueues         string // yarn.scheduler.capacity.root.queues
	DefaultCapacity    int    // yarn.scheduler.capacity.root.default.capacity
	DefaultMaxCapacity int    // yarn.scheduler.capacity.root.default.maximum-capacity
	DefaultState       string // yarn.scheduler.capacity.root.default.state
	Extra              []Property
}

// Clone creates a deep copy
func (c *CapacitySchedulerConfig) Clone() *CapacitySchedulerConfig {
	if c == nil {
		return nil
	}
	clone := *c
	clone.Extra = append([]Property{}, c.Extra...)
	return &clone
}

// ToProperties converts config to a list of properties
func (c *CapacitySchedulerConfig) ToProperties(ctx *TemplateContext) []Property {
	props := []Property{
		{Name: "yarn.scheduler.capacity.root.queues", Value: c.RootQueues},
		{Name: "yarn.scheduler.capacity.root.default.capacity", Value: strconv.Itoa(c.DefaultCapacity)},
		{Name: "yarn.scheduler.capacity.root.default.maximum-capacity", Value: strconv.Itoa(c.DefaultMaxCapacity)},
		{Name: "yarn.scheduler.capacity.root.default.state", Value: c.DefaultState},
	}
	return appendExtraProperties(props, c.Extra, ctx)
}

// Helper functions

func boolToString(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func appendExtraProperties(props []Property, extra []Property, ctx *TemplateContext) []Property {
	for _, p := range extra {
		props = append(props, Property{
			Name:  p.Name,
			Value: ctx.Substitute(p.Value),
		})
	}
	return props
}
