package com.byxf.hbase;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Description:用于 spring boot 的HBase配置信息
 */
@ConfigurationProperties(prefix = "hbase.config")
public class SpringBootHBaseConfig {

    private String zookeeperQuorum;
    private String zookeeperClientPort;


    public String getZookeeperQuorum() {
        return zookeeperQuorum;
    }

    public void setZookeeperQuorum(String zookeeperQuorum) {
        this.zookeeperQuorum = zookeeperQuorum;
    }

    public String getZookeeperClientPort() {
        return zookeeperClientPort;
    }

    public void setZookeeperClientPort(String zookeeperClientPort) {
        this.zookeeperClientPort = zookeeperClientPort;
    }
}
