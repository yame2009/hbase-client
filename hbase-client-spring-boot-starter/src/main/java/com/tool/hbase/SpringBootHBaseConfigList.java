package com.tool.hbase;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Description:用于 spring boot 的多个HBase配置信息
 */
@ConfigurationProperties(prefix = "multiple.hbase.config")
public class SpringBootHBaseConfigList {

	private List<String> zookeeperQuorum;
	private List<String> zookeeperClientPort;

	public List<String> getZookeeperQuorum() {
		return zookeeperQuorum;
	}

	public void setZookeeperQuorum(List<String> zookeeperQuorum) {
		this.zookeeperQuorum = zookeeperQuorum;
	}

	public List<String> getZookeeperClientPort() {
		return zookeeperClientPort;
	}

	public void setZookeeperClientPort(List<String> zookeeperClientPort) {
		this.zookeeperClientPort = zookeeperClientPort;
	}
}
