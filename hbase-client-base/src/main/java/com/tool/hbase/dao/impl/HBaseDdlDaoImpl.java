package com.tool.hbase.dao.impl;

/**
 * @包路径 com.byxf.hbase.dao.impl
 * @创建人 huangbing
 * @创建时间 2018/11/22
 * @描述：
 */

/**
 *
 * @author huangbing
 * @create 2018-11-22 11:52
 * @since 1.0.0
 **/

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Resource;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.tool.hbase.config.HBaseFactoryBean;
import com.tool.hbase.constants.HBaseConstant;
import com.tool.hbase.dao.HBaseDdlDao;

@Service("hBaseDdlDao")
public class HBaseDdlDaoImpl implements HBaseDdlDao {

	private Logger logger = LoggerFactory.getLogger(HBaseDaoImpl.class);

	private volatile HBaseAdmin admin = null;

	@Resource
	private HBaseFactoryBean hBaseFactoryBean;

	private HConnection getConnection() {
		return hBaseFactoryBean.getDefaultConnection();
	}

	// 获取管理员对象
	private HBaseAdmin getAdmin() throws IOException {
		if (admin == null) {
			synchronized (HBaseDdlDaoImpl.class) {
				if (admin == null) {
					try {
						admin = new HBaseAdmin(getConnection().getConfiguration());
					} catch (IOException e) {
						throw e;
					}
				}
			}
		}
		return admin;
	}

	// 关闭管理员对象
	@Override
	public void closeAdmin() throws IOException {
		if (admin != null) {
			admin.close();
		}
	}

	@Override
	public TableName getTableNameObj(String namespace, String tableName) {
		if (StringUtils.isBlank(tableName)) {
			logger.error("hbase tableName is null");
			throw new RuntimeException("hbase tableName is null");
		}

		TableName tableNameObj = null;
		if (StringUtils.isBlank(namespace)) {
			tableNameObj = TableName.valueOf(tableName);
		} else {
			// 表名
			tableNameObj = TableName.valueOf(namespace, tableName);
		}
		return tableNameObj;
	}

	@Override
	public HTable getTable(String namespace, String tableName) throws IOException {
		TableName tableNameObj = getTableNameObj(namespace, tableName);
		HTable hTable = (HTable) getConnection().getTable(tableNameObj);
		return hTable;
	}

	/**
	 * getTableDescriptorList:(获取某一命名空间下所有表). <br/>
	 * 
	 * @param namespace
	 *            命名空间
	 * @return HTableDescriptor[]
	 * @throws Exception
	 */
	@Override
	public HTableDescriptor[] getTableDescriptorList(String namespace) throws Exception {
		if (StringUtils.isBlank(namespace)) {
			logger.error("hbase根据namespace下所有表失败，请求参数namespace为空");
			return null;
		}
		return getAdmin().listTableDescriptorsByNamespace(namespace);
	}

	/**
	 * getTableNameList:(获取某一命名空间下所有表). <br/>
	 * 
	 * @param namespace
	 *            命名空间
	 * @return TableName[]
	 * @throws Exception
	 */
	@Override
	public TableName[] getTableNameList(String namespace) throws Exception {
		if (StringUtils.isBlank(namespace)) {
			logger.error("hbase根据namespace下所有表失败，请求参数namespace为空");
			return null;
		}
		return getAdmin().listTableNamesByNamespace(namespace);
	}

	/**
	 * getAllTables:(根据表名称，查询所有表). <br/>
	 * 
	 * @param tableName
	 *            表名称数组
	 * @return Map<tableName, List<HTableDescriptor>>
	 * @throws Exception
	 */
	@Override
	public Map<String, List<HTableDescriptor>> getAllTables(String... tableName) throws Exception {

		if (tableName == null || tableName.length == 0) {
			return null;
		}

		Set<String> tnSet = new HashSet<String>();
		for (String s : tableName) {
			tnSet.add(s);
		}

		// 获取列簇的描述信息
		HTableDescriptor[] listTables = getAllTables();
		if (listTables == null || listTables.length == 0) {
			return null;
		}

		Map<String, List<HTableDescriptor>> needTableMap = new HashMap<String, List<HTableDescriptor>>();
		for (HTableDescriptor tbDesc : listTables) {
			// 转化为表名
			String tbName = tbDesc.getNameAsString();
			if (!tnSet.contains(tbName)) {
				continue;
			}

			if (needTableMap.containsKey(tbName)) {
				List<HTableDescriptor> oldList = needTableMap.get(tbName);
				oldList.add(tbDesc);
			} else {
				List<HTableDescriptor> list = new ArrayList<HTableDescriptor>();
				list.add(tbDesc);
				needTableMap.put(tbName, list);
			}

		}

		return needTableMap;

	}

	/**
	 * 查询所有表<br/>
	 * 
	 * @see HBaseDdlDao#getAllTables()
	 */
	public HTableDescriptor[] getAllTables() throws IOException {
		HTableDescriptor[] listTables = getAdmin().listTables();
		return listTables;
	}

	// 查看表的列簇属性
	@Override
	public HColumnDescriptor[] getTableDescriptor(String namespace, String tableName) throws Exception {
		if (tableName == null || "".equals(tableName)) {
			logger.info("hbaseTable={}创建失败,表名称为空", tableName);
			return null;
		}

		// 转化为表名
		TableName name = getTableNameObj(namespace, tableName);

		// 判断表是否存在
		if (getAdmin().tableExists(name)) {
			// 获取表中列簇的描述信息
			HTableDescriptor tableDescriptor = getAdmin().getTableDescriptor(name);
			// 获取列簇中列的信息
			HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
			return columnFamilies;

		}

		logger.info("hbaseTable={}不存在", tableName);
		return null;
	}

	/**
	 * getNameSpace:(获取命名空间对象). <br/>
	 *
	 * @param namespace
	 *            命名空间
	 * @return NamespaceDescriptor
	 * @throws Exception
	 */
	@Override
	public NamespaceDescriptor getNameSpace(String namespace) throws Exception {
		NamespaceDescriptor namespaceDescriptor = null;
		try {
			namespaceDescriptor = getAdmin().getNamespaceDescriptor(namespace);
		} catch (Exception e) {
			logger.error("获取hbase命名空间namespace=" + namespace + "异常=" + e.getMessage(), e);
			throw e;
		}
		return namespaceDescriptor;
	}

	/**
	 * getNameSpace:(获取命名空间对象列表). <br/>
	 * 
	 * @throws Exception
	 */
	@Override
	public NamespaceDescriptor[] getNameSpace() throws Exception {
		NamespaceDescriptor[] namespaceDescriptor = null;
		try {
			namespaceDescriptor = getAdmin().listNamespaceDescriptors();
		} catch (Exception e) {
			logger.error("获取hbase命名空间列表异常=" + e.getMessage(), e);
			throw e;
		}
		return namespaceDescriptor;
	}

	/**
	 * createNameSpace:(创建命名空间). <br/>
	 *
	 * @param namespace
	 *            命名空间
	 * @return boolean true,标识创建失败；false，标识创建失败。
	 * @throws Exception
	 * @since JDK 1.7
	 */
	@Override
	public boolean createNameSpace(String namespace) throws Exception {
		NamespaceDescriptor namespaceDescriptor = null;
		try {
			namespaceDescriptor = getNameSpace(namespace);
		} catch (Exception e) {
			logger.error("hbase命名空间namespace=" + namespace + "不存在。异常=" + e.getMessage(), e);
		}

		if (namespaceDescriptor == null) {
			getAdmin().createNamespace(NamespaceDescriptor.create(namespace).build());

			if (getNameSpace(namespace) != null) {
				logger.info("hbase命名空间{}创建完成", namespace);
				return true;
			} else {
				logger.debug("hbase命名空间{}创建失败", namespace);
				return false;
			}
		} else {
			logger.debug("hbase命名空间{}已存在", namespace);
			return true;
		}
	}

	/**
	 * createTable:(根据tbName 名称创建新表). <br/>
	 *
	 * @author huangbing
	 * @param tbName
	 *            表名称
	 * @return boolean，true成功。
	 * @throws Exception
	 */
	@Override
	public boolean createTable(String namespace, String tableName) throws Exception {
		return createTable(namespace, tableName, null);
	}

	/**
	 * createTable:(创建表). <br/>
	 *
	 * @param namespace
	 *            命名空间
	 * @param tableName
	 *            表名称
	 * @param family
	 *            列族
	 * @return boolean，true成功。
	 * @throws Exception
	 * @since JDK 1.7
	 */
	@Override
	public boolean createTable(String namespace, String tableName, String[] family) throws Exception {

		if (tableName == null || "".equals(tableName)) {
			logger.info("namespace={}创建失败,表名称tableName为空", namespace);
			return false;
		}

		boolean isCreateNameSpace = createNameSpace(namespace);
		if (!isCreateNameSpace) {
			return false;
		}

		TableName name = getTableNameObj(namespace, tableName);
		// 判断表是否存在
		if (getAdmin().tableExists(name)) {
			logger.info("hbaseTable={}已经存在！", name);
			return true;
		} else {
			// 表的列簇示例
			HTableDescriptor htd = new HTableDescriptor(name);

			if (family != null && family.length > 0) {
				// 向列簇中添加列的信息
				for (String str : family) {
					HColumnDescriptor hcd = new HColumnDescriptor(str);
					// Compression 压缩. SNAPPY从压缩率和压缩速度来看，性价比最高。
					hcd.setCompressionType(Algorithm.SNAPPY);
					htd.addFamily(hcd);
				}
			}

			return createTable(htd);
		}

	}

	/**
	 * createTable:(创建新表). <br/>
	 * 创建表，传参:封装好的多个列簇
	 *
	 * @param htds
	 *            HTableDescriptor对象
	 * @return boolean，true成功。
	 * @throws Exception
	 * @see HBaseDdlDao#createTable(org.apache.hadoop.hbase.HTableDescriptor)
	 */
	@Override
	public boolean createTable(HTableDescriptor htds) throws Exception {
		if (htds == null) {
			return false;
		}

		// 创建表
		try {
			setDefaultColumnFamilies(htds);
			getAdmin().createTable(htds);
		} catch (Exception e) {
			logger.error("hbase createTable 异常,tableName=" + htds.getNameAsString() + ",异常=" + e.getMessage(), e);
			return false;
		}

		String tbName = htds.getNameAsString();
		// 判断表是否创建成功
		if (getAdmin().tableExists(tbName)) {
			logger.info("hbaseTable={}创建成功", tbName);
			return true;
		} else {
			logger.info("hbaseTable={}创建失败", tbName);
			return false;
		}
	}

	private void setDefaultColumnFamilies(HTableDescriptor htds) {
		HColumnDescriptor[] columnFamilies = htds.getColumnFamilies();
		if (columnFamilies == null || columnFamilies.length == 0) {
			HColumnDescriptor addColumnDescriptor = new HColumnDescriptor(HBaseConstant.DEFAULT_FAMILY);
			// Compression 压缩. SNAPPY从压缩率和压缩速度来看，性价比最高。
			addColumnDescriptor.setCompressionType(Algorithm.SNAPPY);
			htds.addFamily(addColumnDescriptor);
		}
	}

	/**
	 * 
	 * existTable:(是否存在表). <br/>
	 *
	 * @param tableName
	 *            表名称
	 * @return boolean，true，存在；false，不存在。
	 * @throws Exception
	 */
	@Override
	public boolean existTable(String namespace, String tableName, String[] family, boolean isNotExistForCreate) throws Exception {
		return existTable(namespace, tableName, family, isNotExistForCreate, isNotExistForCreate);
	}

	/**
	 * 
	 * existTable:(是否存在表). <br/>
	 *
	 * @param tableName
	 *            表名称
	 * @param isNotExistForCreate
	 *            true: 标识不存在，则新建表；
	 * @return boolean，true，存在；false，不存在。
	 * @throws Exception
	 */
	@Override
	public boolean existTable(String namespace, String tableName, String[] family, boolean isNotExistForCreate, boolean isSetEnableTable) throws Exception {
		if (existTable(namespace, tableName)) {
			if (isSetEnableTable && isTableDisabled(namespace, tableName)) {
				// 如果tableName为不可用状态，则设置为可用状态
				enableTable(namespace, tableName);
			}
			return true;
		} else {
			// 不存在，则创建。
			if (isNotExistForCreate) {
				return createTable(namespace, tableName, family);
			}
		}

		return false;
	}

	// 判断表存在不存在
	@Override
	public boolean existTable(String namespace, String tableName) throws Exception {
		try {
			TableName name = getTableNameObj(namespace, tableName);
			return getAdmin().tableExists(name);
		} catch (Exception e) {
			logger.error("existTable判断hbase表是否存在异常", e);
			return false;
		}
	}

	// disable表
	@Override
	public void disableTable(String namespace, String tableName) throws Exception {

		if (tableName == null || "".equals(tableName)) {
			logger.info("hbaseTable={}设置失败,表名称为空", tableName);
		}

		TableName name = getTableNameObj(namespace, tableName);

		if (getAdmin().tableExists(name)) {
			if (isTableEnabled(name)) {
				getAdmin().disableTable(name);
			} else {
				logger.info("hbaseTable={}不是活动状态", tableName);
			}
		} else {
			logger.info("hbaseTable={}不存在", tableName);
		}

	}

	// enableTable表
	@Override
	public void enableTable(String namespace, String tableName) throws Exception {

		if (tableName == null || "".equals(tableName)) {
			logger.info("hbaseTable={}设置失败,表名称为空", tableName);
		}

		TableName name = getTableNameObj(namespace, tableName);

		if (getAdmin().tableExists(name)) {
			if (isTableDisabled(name)) {
				logger.info("hbaseTable={}不是活动状态，设置为活动状态", tableName);
				getAdmin().enableTable(name);
			} else {
				logger.info("hbaseTable={}是活动状态", tableName);
			}
		} else {
			logger.info("hbaseTable={}不存在", tableName);
		}

	}

	/**
	 * isTableEnabled:(判断当前表是否为可用状态). <br/>
	 *
	 * @param name
	 *            tableName name of table to check
	 * @return true if table is on-line
	 * @throws IOException
	 *             - if a remote or network exception occurs
	 */
	@Override
	public boolean isTableEnabled(String namespace, String tableName) throws IOException {
		return isTableEnabled(getTableNameObj(namespace, tableName));
	}

	/**
	 * isTableEnabled:(判断当前表是否为可用状态). <br/>
	 *
	 * @param name
	 *            tableName name of table to check
	 * @return true if table is on-line
	 * @throws IOException
	 *             - if a remote or network exception occurs
	 */
	@Override
	public boolean isTableEnabled(TableName tableName) throws IOException {
		return getAdmin().isTableEnabled(tableName);
	}

	/**
	 * isTableEnabled:(判断当前表是否为不可用状态). <br/>
	 *
	 * @param name
	 *            tableName name of table to check
	 * @return true if table is off-line
	 * @throws IOException
	 *             - if a remote or network exception occurs
	 */
	@Override
	public boolean isTableDisabled(String namespace, String tableName) throws IOException {
		return isTableDisabled(getTableNameObj(namespace, tableName));
	}

	/**
	 * isTableEnabled:(判断当前表是否为不可用状态). <br/>
	 *
	 * @param name
	 *            tableName name of table to check
	 * @return true if table is off-line
	 * @throws IOException
	 *             - if a remote or network exception occurs
	 */
	@Override
	public boolean isTableDisabled(TableName tableName) throws IOException {
		return getAdmin().isTableDisabled(tableName);
	}

	// drop表
	@Override
	public boolean dropTable(String namespace, String tableName) throws Exception {
		if (tableName == null || "".equals(tableName)) {
			logger.info("hbaseTable={}设置失败,表名称为空", tableName);
		}

		// 转化为表名
		TableName name = getTableNameObj(namespace, tableName);
		// 判断表是否存在
		if (getAdmin().tableExists(name)) {
			// 判断表是否处于可用状态
			boolean tableEnabled = isTableEnabled(name);

			if (tableEnabled) {
				// 使表变成不可用状态
				getAdmin().disableTable(name);
			}

			// 删除表
			getAdmin().deleteTable(name);
			// 判断表是否存在
			if (getAdmin().tableExists(name)) {
				logger.info("hbaseTable={}删除失败", tableName);
				return false;
			} else {
				logger.info("hbaseTable={}删除成功", tableName);
				return true;
			}

		} else {
			logger.info("hbaseTable={}不存在", tableName);
			return true;
		}
	}

	// 修改表(增加和删除)
	@Override
	public void modifyTable(String namespace, String tableName) throws Exception {
		// 转化为表名
		TableName name = getTableNameObj(namespace, tableName);
		// 判断表是否存在
		if (getAdmin().tableExists(name)) {
			// 判断表是否可用状态
			boolean tableEnabled = isTableEnabled(name);

			if (tableEnabled) {
				// 使表变成不可用
				getAdmin().disableTable(name);
			}
			// 根据表名得到表
			HTableDescriptor tableDescriptor = getAdmin().getTableDescriptor(name);
			// 创建列簇结构对象
			HColumnDescriptor columnFamily1 = new HColumnDescriptor("cf1".getBytes());
			HColumnDescriptor columnFamily2 = new HColumnDescriptor("cf2".getBytes());

			// Compression 压缩. SNAPPY从压缩率和压缩速度来看，性价比最高。
			columnFamily1.setCompressionType(Algorithm.SNAPPY);
			columnFamily2.setCompressionType(Algorithm.SNAPPY);

			tableDescriptor.addFamily(columnFamily1);
			tableDescriptor.addFamily(columnFamily2);
			// 替换该表所有的列簇
			getAdmin().modifyTable(name, tableDescriptor);

		} else {
			logger.info("hbaseTable={}不存在", tableName);
		}
	}

	// 修改表(增加和删除)
	@Override
	public void modifyTable(String namespace, String tableName, String[] addColumn, String[] removeColumn) throws Exception {
		// 转化为表名
		TableName name = getTableNameObj(namespace, tableName);
		// 判断表是否存在
		if (getAdmin().tableExists(name)) {
			// 判断表是否可用状态
			boolean tableEnabled = isTableEnabled(name);

			if (tableEnabled) {
				// 使表变成不可用
				getAdmin().disableTable(name);
			}
			// 根据表名得到表
			HTableDescriptor tableDescriptor = getAdmin().getTableDescriptor(name);
			// 创建列簇结构对象，添加列
			for (String add : addColumn) {
				HColumnDescriptor addColumnDescriptor = new HColumnDescriptor(add);
				// Compression 压缩. SNAPPY从压缩率和压缩速度来看，性价比最高。
				addColumnDescriptor.setCompressionType(Algorithm.SNAPPY);
				tableDescriptor.addFamily(addColumnDescriptor);
			}
			// 创建列簇结构对象，删除列
			for (String remove : removeColumn) {
				HColumnDescriptor removeColumnDescriptor = new HColumnDescriptor(remove);
				tableDescriptor.removeFamily(removeColumnDescriptor.getName());
			}

			getAdmin().modifyTable(name, tableDescriptor);

		} else {
			logger.info("hbaseTable={}不存在", tableName);
		}

	}

	@Override
	public void modifyTable(String namespace, String tableName, HColumnDescriptor hcds) throws Exception {
		// 转化为表名
		TableName name = getTableNameObj(namespace, tableName);

		// 根据表名得到表
		HTableDescriptor tableDescriptor = getAdmin().getTableDescriptor(name);
		// 获取表中所有的列簇信息
		HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();

		boolean flag = false;
		// 判断参数中传入的列簇是否已经在表中存在
		for (HColumnDescriptor columnFamily : columnFamilies) {
			if (columnFamily.equals(hcds)) {
				flag = true;
			}
		}
		// 存在提示，不存在直接添加该列簇信息
		if (flag) {
			logger.info("该列簇已经存在");
		} else {
			tableDescriptor.addFamily(hcds);
			getAdmin().modifyTable(name, tableDescriptor);
		}

	}

	// 根据rowkey查询数据
	@Override
	public Result getResult(String namespace, String tableName, String rowKey) throws Exception {

		Result result = null;
		TableName name = getTableNameObj(namespace, tableName);
		if (getAdmin().tableExists(name)) {
			HTable table = (HTable) getConnection().getTable(name);
			Get get = new Get(rowKey.getBytes());
			result = table.get(get);

		} else {
			result = null;
		}

		return result;
	}

	// 根据rowkey查询数据
	@Override
	public Result getResult(String namespace, String tableName, String rowKey, String familyName) throws Exception {
		Result result;
		TableName name = getTableNameObj(namespace, tableName);
		if (getAdmin().tableExists(name)) {
			HTable table = (HTable) getConnection().getTable(name);
			Get get = new Get(rowKey.getBytes());
			get.addFamily(familyName.getBytes());
			result = table.get(get);

		} else {
			result = null;
		}

		return result;
	}

	// 根据rowkey查询数据
	@Override
	public Result getResult(String namespace, String tableName, String rowKey, String familyName, String columnName) throws Exception {

		Result result;
		TableName name = getTableNameObj(namespace, tableName);
		if (getAdmin().tableExists(name)) {
			HTable table = (HTable) getConnection().getTable(name);
			Get get = new Get(rowKey.getBytes());
			get.addColumn(familyName.getBytes(), columnName.getBytes());
			result = table.get(get);

		} else {
			result = null;
		}

		return result;
	}

	// 查询指定version
	@Override
	public Result getResultByVersion(String namespace, String tableName, String rowKey, String familyName, String columnName, int versions) throws Exception {
		Result result;
		TableName name = getTableNameObj(namespace, tableName);
		if (getAdmin().tableExists(name)) {
			HTable table = (HTable) getConnection().getTable(name);
			Get get = new Get(rowKey.getBytes());
			get.addColumn(familyName.getBytes(), columnName.getBytes());
			get.setMaxVersions(versions);
			result = table.get(get);

		} else {
			result = null;
		}

		return result;
	}

	// scan全表数据
	@Override
	public ResultScanner getResultScann(String namespace, String tableName) throws Exception {

		ResultScanner result;
		TableName name = getTableNameObj(namespace, tableName);
		if (getAdmin().tableExists(name)) {
			HTable table = (HTable) getConnection().getTable(name);
			Scan scan = new Scan();
			result = table.getScanner(scan);

		} else {
			result = null;
		}

		return result;
	}

	// scan全表数据
	@Override
	public ResultScanner getResultScann(String namespace, String tableName, Scan scan) throws Exception {

		ResultScanner result;
		TableName name = getTableNameObj(namespace, tableName);
		if (getAdmin().tableExists(name)) {
			HTable table = (HTable) getConnection().getTable(name);
			result = table.getScanner(scan);

		} else {
			result = null;
		}

		return result;
	}

	// 删除数据（指定的列）
	@Override
	public void deleteColumn(String namespace, String tableName, String rowKey) throws Exception {

		TableName name = getTableNameObj(namespace, tableName);
		if (getAdmin().tableExists(name)) {
			HTable table = (HTable) getConnection().getTable(name);
			Delete delete = new Delete(rowKey.getBytes());
			table.delete(delete);

		} else {
			logger.info("hbaseTable={}不存在", tableName);
		}

	}

	// 删除数据（指定的列）
	@Override
	public void deleteColumn(String namespace, String tableName, String rowKey, String falilyName) throws Exception {

		TableName name = getTableNameObj(namespace, tableName);
		if (getAdmin().tableExists(name)) {
			HTable table = (HTable) getConnection().getTable(name);
			Delete delete = new Delete(rowKey.getBytes());
			delete.deleteFamily(falilyName.getBytes());
			table.delete(delete);

		} else {
			logger.info("hbaseTable={}不存在", tableName);
		}

	}

	// 删除数据（指定的列）
	@Override
	public void deleteColumn(String namespace, String tableName, String rowKey, String falilyName, String columnName) throws Exception {
		TableName name = getTableNameObj(namespace, tableName);
		if (getAdmin().tableExists(name)) {
			HTable table = (HTable) getConnection().getTable(name);
			Delete delete = new Delete(rowKey.getBytes());
			delete.deleteColumn(falilyName.getBytes(), columnName.getBytes());
			table.delete(delete);

		} else {
			logger.info("hbaseTable={}不存在", tableName);
		}
	}

}