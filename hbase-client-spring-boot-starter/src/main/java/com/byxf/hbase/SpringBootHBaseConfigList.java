package com.byxf.hbase;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

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
