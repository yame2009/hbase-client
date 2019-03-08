package com.byxf.hbase.config;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.byxf.hbase.constants.HBaseConstant;

/**
 * <p>
 * HBaseConstant 配置载入。初始化连接。 <br/>
 * HBaseFactoryBean：HBase的连接初始化工厂Bean，用于初始化HBase连接<br/>
 * 加入spring容器，已在spring-hbase.xml文件中配置。这里不用添加注解。
 */
public class HBaseFactoryBean {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	private  List<HConnection> connections;

	private List<HBaseConfig> hbaseConfigs;

	/**
	 * the thread pool to use for batch operation in HTables used via this
	 * HConnection<br/>
	 */
	private int threadPoolNum = 20;

	// 约定 hbase系统前缀标识
	private final String HBASE_PREFIX = "hbase";
	// hTables Thread Pool Num
	private final String HTABLES_THREAD_POOL_NUM = "hbase.hTables.thread.pool.num";
	// hbase 连接zookeeper的地址
	private final String ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";

	public void setHbaseConfigs(List<HBaseConfig> hbaseConfigs) {
		logger.info("初始化连接HBase,hbaseConfigs={}",hbaseConfigs);
		this.hbaseConfigs = hbaseConfigs;
	}

	public void initializeConnections() throws Exception {
		connections = new ArrayList<>();

		// 配置参数为空，从系统环境变量中初始化
		if (CollectionUtils.isEmpty(hbaseConfigs)) {
			// 直接读取属性文件中约定的hbase前缀属性，获取hbase连接参数
			setHBaseViaProperties();

		} else {
			// 从xml获取hbase配置参数时，使用此方法
			setHBaseConfigs();
		}
	}

	/**
	 * 直接读取属性文件中约定的hbase前缀属性，获取hbase连接参数
	 *
	 * @author huangbing
	 * @throws IOException
	 * @since JDK 1.7
	 */
	private void setHBaseViaProperties() throws IOException {
		Configuration configuration = HBaseConfiguration.create();
		
		// String property2 =
		// PropertyPlaceholder.getProperty("hbase.hbase.zookeeper.quorum");
		// String property = System.getProperty("hbase.hbase.zookeeper.quorum");
		// Properties properties = System.getProperties();

		Map<String, String> properties = PropertyPlaceholder.getProperties();
		for (Entry<String, String> t : properties.entrySet()) {
			String key = String.valueOf(t.getKey());
			String value = String.valueOf(t.getValue());
			if (StringUtils.isBlank(key)) {
				continue;
			}

			// 如果 hbase htable 默认线程有设置，则也配置为主
			if (HTABLES_THREAD_POOL_NUM.equalsIgnoreCase(key)) {
				int num = NumberUtils.toInt(value);
				threadPoolNum = num == 0 ? 20 : num;
				continue;
			}

			// 查找hbase约定的前缀属性
			if (key.startsWith(HBASE_PREFIX)) {
				String hbaseKey = key.replaceFirst(HBASE_PREFIX + ".", "");
				configuration.set(hbaseKey, value);
			}
		}
		
		logger.info("初始化连接HBase,连接参数={}", configuration);

		String zookeeperQuorum = configuration.get(ZOOKEEPER_QUORUM);
		if (StringUtils.isBlank(zookeeperQuorum)) {
			logger.error("link hbase and create HConnection is fail, {} is empty", ZOOKEEPER_QUORUM);
			throw new RuntimeException("link hbase and create HConnection is fail, " + ZOOKEEPER_QUORUM + " is empty");
		}

		ExecutorService pool = Executors.newScheduledThreadPool(threadPoolNum); // 设置连接池
		HConnection connection = HConnectionManager.createConnection(configuration, pool);
		connections.add(connection);
	}

	/**
	 * 
	 * 从spring xml获取hbase配置参数时，使用此方法,采取属性注解的方式注入属性值<br/>
	 *
	 * @throws IOException
	 * @since JDK 1.7
	 */
	private void setHBaseConfigs() throws IOException {

		for (HBaseConfig config : hbaseConfigs) {
			logger.info("初始化连接HBase,连接参数config={}", config);

			Configuration configuration = HBaseConfiguration.create();

			if (StringUtils.isBlank(config.getZookeeperQuorum())) {
				continue;
			}

			configuration.set("hbase.zookeeper.quorum", config.getZookeeperQuorum());
			configuration.set("hbase.zookeeper.property.clientPort",
					StringUtils.isBlank(config.getZookeeperClientPort()) ? HBaseConstant.DEFAULT_HBASE_PORT : config.getZookeeperClientPort());

			if (StringUtils.isNotBlank(config.getZookeeperZnodeParent())) {
				configuration.set("zookeeper.znode.parent", config.getZookeeperZnodeParent());
			}
			if (StringUtils.isNotBlank(config.getHbaseNamespace())) {
				configuration.set("hbase.namespace", config.getHbaseNamespace());
			}

			int rpcTimeout = config.getRpcTimeout() <= 0 ? 20000 : config.getRpcTimeout();
			// HBase RPC请求超时时间，默认60s(60000),时间单位：毫秒,可设置20000。
			configuration.setInt("hbase.rpc.timeout", rpcTimeout);

			int clientRetriesNumber = config.getClientRetriesNumber() <= 0 ? 10 : config.getClientRetriesNumber();
			// 客户端重试最大次数，默认35.可设置为10。
			configuration.setInt("hbase.client.retries.number", clientRetriesNumber);

			int clientOperationTimeout = config.getClientOperationTimeout() <= 0 ? 30000 : config.getClientOperationTimeout();
			// 客户端发起一次操作数据请求直至得到响应之间的总超时时间，可能包含多个RPC请求，默认为2min,时间单位：毫秒。可设置为30000。
			configuration.setInt("hbase.client.operation.timeout", clientOperationTimeout);

			int clientScannerTimeoutPeriod = config.getClientScannerTimeoutPeriod() <= 0 ? 200000 : config.getClientScannerTimeoutPeriod();
			// 客户端发起一次scan操作的rpc调用至得到响应之间的总超时时间,时间单位：毫秒。可设置为200000。
			configuration.setInt("hbase.client.scanner.timeout.period", clientScannerTimeoutPeriod);

			logger.info("初始化连接HBase,连接参数={}", configuration);

			int tempPoolNum = config.getThreadPoolNum();
			if (tempPoolNum <= 0) {
				tempPoolNum = threadPoolNum;
			}

			ExecutorService pool = Executors.newScheduledThreadPool(tempPoolNum); // 设置连接池
			HConnection connection = HConnectionManager.createConnection(configuration, pool);
			connections.add(connection);
		}
	}

	public  HConnection getDefaultConnection() {
		return connections.get(0);
	}

	/**
	 * 关闭连接池
	 */
	public void close() {
		try {
			if (getDefaultConnection() != null)
				getDefaultConnection().close();
		} catch (IOException e) {
			logger.error("hbase close HConnection is  IOException");
			throw new RuntimeException("hbase close HConnection is  IOException", e);
		}
	}

	public  HConnection getSpecifyConnection(int index) {
		if (index > connections.size() - 1) {
			throw new RuntimeException("hbase connection is not exist");
		}
		return connections.get(index);
	}
}
