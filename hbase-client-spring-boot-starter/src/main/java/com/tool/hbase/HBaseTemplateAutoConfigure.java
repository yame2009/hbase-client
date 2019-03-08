package com.tool.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.tool.hbase.config.HBaseConfig;
import com.tool.hbase.config.HBaseFactoryBean;
import com.tool.hbase.constants.HBaseConstant;
import com.tool.hbase.dao.HBaseDao;
import com.tool.hbase.dao.impl.HBaseDaoImpl;

/**
 * Description:配置基类
 */
@Configuration
@ConditionalOnClass(SpringBootHBaseConfig.class)
@EnableConfigurationProperties({ SpringBootHBaseConfig.class, SpringBootHBaseConfigList.class })
public class HBaseTemplateAutoConfigure {

	@Autowired
	private SpringBootHBaseConfig springBootHBaseConfig;
	@Autowired
	private SpringBootHBaseConfigList springBootHBaseConfigList;

	@Autowired
	private HBaseFactoryBean hBaseFactoryBean;

	@Bean
	public HBaseFactoryBean initializeHBaseFactoryBean() throws Exception {
		List<HBaseConfig> list = new ArrayList<>();
		// single HBase
		if (StringUtils.isNotBlank(springBootHBaseConfig.getZookeeperQuorum())) {
			HBaseConfig config = new HBaseConfig();
			config.setZookeeperQuorum(springBootHBaseConfig.getZookeeperQuorum());
			config.setZookeeperClientPort(
					StringUtils.isBlank(springBootHBaseConfig.getZookeeperClientPort()) ? HBaseConstant.DEFAULT_HBASE_PORT : springBootHBaseConfig.getZookeeperClientPort());
			list.add(config);
		}
		// multiple HBase
		List<String> zookeeperConfigQuorums = springBootHBaseConfigList.getZookeeperQuorum();
		if (zookeeperConfigQuorums != null && zookeeperConfigQuorums.size() > 0) {
			for (int i = 0; i < zookeeperConfigQuorums.size(); i++) {
				HBaseConfig config = new HBaseConfig();
				config.setZookeeperQuorum(zookeeperConfigQuorums.get(i));
				String zookeeperClientPort = HBaseConstant.DEFAULT_HBASE_PORT;
				if (springBootHBaseConfigList.getZookeeperClientPort() != null && StringUtils.isNotBlank(springBootHBaseConfigList.getZookeeperClientPort().get(i))) {
					zookeeperClientPort = springBootHBaseConfigList.getZookeeperClientPort().get(i);
				}
				config.setZookeeperClientPort(zookeeperClientPort);
				list.add(config);
			}
		}
		hBaseFactoryBean.setHbaseConfigs(list);
		hBaseFactoryBean.initializeConnections();
		return hBaseFactoryBean;
	}

	@Bean
	@ConditionalOnMissingBean(HBaseDao.class)
	public HBaseDaoImpl getHBaseDao() {
		return new HBaseDaoImpl();
	}

}
