package com.byxf.hbase.dao;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public interface HBaseDdlDao {

	/**
	 * 
	 * closeAdmin:(关闭管理员对象). <br/>
	 *
	 * @author huangbing
	 * @throws IOException
	 * @since JDK 1.7
	 */
	public void closeAdmin() throws IOException;

	public void deleteColumn(String namespace, String tableName, String rowKey, String falilyName, String columnName) throws Exception;

	public void deleteColumn(String namespace, String tableName, String rowKey, String falilyName) throws Exception;

	public void deleteColumn(String namespace, String tableName, String rowKey) throws Exception;

	public ResultScanner getResultScann(String namespace, String tableName, Scan scan) throws Exception;

	public ResultScanner getResultScann(String namespace, String tableName) throws Exception;

	public Result getResultByVersion(String namespace, String tableName, String rowKey, String familyName, String columnName, int versions) throws Exception;

	public Result getResult(String namespace, String tableName, String rowKey, String familyName, String columnName) throws Exception;

	public Result getResult(String namespace, String tableName, String rowKey, String familyName) throws Exception;

	public Result getResult(String namespace, String tableName, String rowKey) throws Exception;

	public void modifyTable(String namespace, String tableName, HColumnDescriptor hcds) throws Exception;

	public void modifyTable(String namespace, String tableName, String[] addColumn, String[] removeColumn) throws Exception;

	public void modifyTable(String namespace, String tableName) throws Exception;

	/**
	 * 
	 * dropTable:(删除表). <br/>
	 *
	 * @param tableName
	 *            表名称
	 * @return boolean，true删除成功；false,删除失败。
	 * @throws Exception
	 */
	public boolean dropTable(String namespace, String tableName) throws Exception;

	/**
	 * 
	 * disableTable:(设置表不可用). <br/>
	 *
	 * @param tableName
	 *            表名称
	 * @throws Exception
	 */
	public void disableTable(String namespace, String tableName) throws Exception;

	/**
	 * 
	 * enableTable:(设置表为可用状态). <br/>
	 *
	 * @param tableName   表名称
	 * @throws Exception
	 */
	public void enableTable(String namespace, String tableName) throws Exception;

	/**
	 * 
	 * existTable:(是否存在表). <br/>
	 *
	 * @param tableName
	 *            表名称
	 * @return boolean，true，存在；false，不存在。
	 * @throws Exception
	 */
	public boolean existTable(String namespace, String tableName) throws Exception;

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
	public boolean existTable(String namespace, String tableName, String[] family, boolean isNotExistForCreate) throws Exception;

	/**
	 * 
	 * existTable:(是否存在表). <br/>
	 * 
	 * @param namespace
	 *            命名空间
	 * @param tableName
	 *            表名称
	 * @param family
	 *            列族
	 * @param isNotExistForCreate
	 *            true: 标识不存在，则新建表；
	 * @param isSetEnableTable
	 *            true: 标识如果表存在且不可用，则设置为可用状态；
	 * @return boolean，true，存在；false，不存在。
	 * @throws Exception
	 */
	public boolean existTable(String namespace, String tableName, String[] family, boolean isNotExistForCreate, boolean isSetEnableTable) throws Exception;

	/**
	 * 
	 * createTable:(根据tbName 名称创建新表). <br/>
	 *
	 * @author huangbing
	 * @param namespace
	 *            namespace可为空
	 * @param tbName
	 *            表名称
	 * @return boolean，true成功。
	 * @throws Exception
	 */
	public boolean createTable(String namespace, String tbName) throws Exception;

	/**
	 * createTable:(创建新表). <br/>
	 *
	 * @param htds
	 *            HTableDescriptor对象
	 * @return boolean，true成功。
	 * @throws Exception
	 */
	public boolean createTable(HTableDescriptor htds) throws Exception;

	/**
	 * 
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
	public boolean createTable(String namespace, String tableName, String[] family) throws Exception;

	/**
	 * createNameSpace:(创建命名空间). <br/>
	 *
	 * @param namespace
	 *            命名空间
	 * @return boolean true,标识创建失败；false，标识创建失败。
	 * @throws Exception
	 */
	public boolean createNameSpace(String namespace) throws Exception;

	/**
	 * getNameSpace:(获取命名空间对象). <br/>
	 *
	 * @param namespace
	 *            命名空间
	 * @throws IOException
	 */
	public NamespaceDescriptor getNameSpace(String namespace) throws Exception;

	/**
	 * getNameSpace:(获取命名空间对象列表). <br/>
	 *
	 * @throws IOException
	 */
	public NamespaceDescriptor[] getNameSpace() throws Exception;

	/**
	 * 
	 * getAllTables:(查询所有表). <br/>
	 *
	 * @return HTableDescriptor[]
	 * @throws Exception
	 * @since JDK 1.7
	 */
	public HTableDescriptor[] getAllTables() throws Exception;

	/**
	 * 
	 * getTableDescriptor:( 查看表的列簇属性). <br/>
	 * 
	 * @param namespace
	 *            namespace,可为空
	 * @param tableName
	 *            表名称
	 * @return HColumnDescriptor[]
	 * @throws Exception
	 */
	public HColumnDescriptor[] getTableDescriptor(String namespace, String tableName) throws Exception;

	/**
	 * getAllTables:(根据表名称，查询所有表). <br/>
	 * 
	 * @param tableName
	 *            表名称数组
	 * @return Map<tableName, List<HTableDescriptor>>
	 * @throws Exception
	 */
	public Map<String, List<HTableDescriptor>> getAllTables(String... tableName) throws Exception;

	/**
	 * getTableNameObj:(获取TableName对象 ). <br/>
	 *
	 * @param namespace
	 *            namespace可为空
	 * @param tableName
	 *            tableName不能为空
	 * @return TableName
	 */
	public TableName getTableNameObj(String namespace, String tableName);

	/**
	 * getTableObj:(获取HTable对象 ). <br/>
	 *
	 * @param namespace
	 *            namespace可为空
	 * @param tableName
	 *            tableName不能为空
	 * @return HTable
	 */
	public HTable getTable(String namespace, String tableName) throws IOException;

	/**
	 * 
	 * getTableList:(获取某一命名空间下所有表). <br/>
	 * 
	 * @param namespace
	 *            命名空间
	 * @return
	 * @throws IOException
	 */
	public HTableDescriptor[] getTableDescriptorList(String namespace) throws Exception;

	/**
	 * getTableNameList:(获取某一命名空间下所有表). <br/>
	 * 
	 * @param namespace
	 *            命名空间
	 * @return TableName[]
	 * @throws Exception
	 */
	public TableName[] getTableNameList(String namespace) throws Exception;

	/**
	 * isTableEnabled:(判断当前表是否为可用状态). <br/>
	 *
	 * @param name
	 *            tableName name of table to check
	 * @return true if table is on-line
	 * @throws IOException
	 *             - if a remote or network exception occurs
	 */
	public boolean isTableEnabled(String namespace, String tableName) throws IOException;

	/**
	 * isTableEnabled:(判断当前表是否为可用状态). <br/>
	 *
	 * @param name
	 *            tableName name of table to check
	 * @return true if table is on-line
	 * @throws IOException
	 *             - if a remote or network exception occurs
	 */
	public boolean isTableEnabled(TableName tableName) throws IOException;

	/**
	 * isTableEnabled:(判断当前表是否为不可用状态). <br/>
	 *
	 * @param name
	 *            tableName name of table to check
	 * @return true if table is off-line
	 * @throws IOException
	 *             - if a remote or network exception occurs
	 */
	public boolean isTableDisabled(String namespace, String tableName) throws IOException;

	/**
	 * isTableEnabled:(判断当前表是否为不可用状态). <br/>
	 *
	 * @param name
	 *            tableName name of table to check
	 * @return true if table is off-line
	 * @throws IOException
	 *             - if a remote or network exception occurs
	 */
	public boolean isTableDisabled(TableName tableName) throws IOException;

}
