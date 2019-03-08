package com.tool.hbase.config;

/**
 * HBase Config
 */
public class HBaseConfig {

	private String zookeeperQuorum;
	private String zookeeperClientPort;

	private String zookeeperZnodeParent;
	private String hbaseNamespace;

	// HBase RPC请求超时时间，默认60s(60000),时间单位：毫秒。
	private int rpcTimeout;

	// 客户端重试最大次数，默认35
	private int clientRetriesNumber;

	// 客户端发起一次操作数据请求直至得到响应之间的总超时时间，可能包含多个RPC请求，默认为2min,时间单位：毫秒。
	private int clientOperationTimeout;

	// 客户端发起一次scan操作的rpc调用至得到响应之间的总超时时间,时间单位：毫秒。
	private int clientScannerTimeoutPeriod;

	/**
	 * the thread pool to use for batch operation in HTables used via this
	 * HConnection<br/>
	 */
	private int threadPoolNum;

	public HBaseConfig() {
	}

	public HBaseConfig(String zookeeperQuorum, String zookeeperClientPort) {
		this.zookeeperQuorum = zookeeperQuorum;
		this.zookeeperClientPort = zookeeperClientPort;
	}

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

	public String getZookeeperZnodeParent() {
		return zookeeperZnodeParent;
	}

	public void setZookeeperZnodeParent(String zookeeperZnodeParent) {
		this.zookeeperZnodeParent = zookeeperZnodeParent;
	}

	public String getHbaseNamespace() {
		return hbaseNamespace;
	}

	public void setHbaseNamespace(String hbaseNamespace) {
		this.hbaseNamespace = hbaseNamespace;
	}

	public int getThreadPoolNum() {
		return threadPoolNum;
	}

	public void setThreadPoolNum(int threadPoolNum) {
		this.threadPoolNum = threadPoolNum;
	}

	public int getRpcTimeout() {
		return rpcTimeout;
	}

	public void setRpcTimeout(int rpcTimeout) {
		this.rpcTimeout = rpcTimeout;
	}

	public int getClientRetriesNumber() {
		return clientRetriesNumber;
	}

	public void setClientRetriesNumber(int clientRetriesNumber) {
		this.clientRetriesNumber = clientRetriesNumber;
	}

	public int getClientOperationTimeout() {
		return clientOperationTimeout;
	}

	public void setClientOperationTimeout(int clientOperationTimeout) {
		this.clientOperationTimeout = clientOperationTimeout;
	}

	public int getClientScannerTimeoutPeriod() {
		return clientScannerTimeoutPeriod;
	}

	public void setClientScannerTimeoutPeriod(int clientScannerTimeoutPeriod) {
		this.clientScannerTimeoutPeriod = clientScannerTimeoutPeriod;
	}

}
