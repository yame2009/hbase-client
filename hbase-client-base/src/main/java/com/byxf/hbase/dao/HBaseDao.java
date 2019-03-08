package com.byxf.hbase.dao;

import java.util.List;

import javax.annotation.Nullable;

import com.byxf.hbase.bean.ColumnInfo;

/**
 * HBase基础操作DML 核心类<br/>
 *
 * <p>
 * 主要核心代码，封装了总共八大类的方法：
 *
 * 1.获取单个PO的get方法，column用于限制返回的column，filter用于批量过滤
 * 2.获取多个PO的getList方法
 * 3.获取单个signleColumn、多个Column的getColumns方法，支持分页（HBase的宽表分页，基于偏移量设计）
 * 4.支持批量PUT的put方法
 * 5.支持批量DELETE的delete方法
 * 6.支持原子操作的addCounter计数器方法
 * 7.支持只获取RowKey的分页方法（基于KeyOnlyFilter，减少数据传输，适用于仅需要RowKey情况）
 * 8.支持getColumsObj适用于value是一个json对象的查询方法
 * </p>
 */
public interface HBaseDao {

	/**
	 * get:(获取数据). <br/>
	 * 
	 * @param namespace
	 *            命名空间
	 * @param tableName
	 *            表
	 * @param rowKey
	 *            row key
	 * @param columns
	 *            列
	 * @param filters
	 *            过滤器
	 * @param clazz
	 *            映射对象
	 * @return <T>
	 * @since JDK 1.7
	 */
	public <T> T get(String namespace, String tableName, String rowKey, @Nullable List<ColumnInfo> columns, @Nullable List<ColumnInfo> filters, Class<? extends T> clazz)
			throws Exception;

	/**
	 * get:(获取数据). <br/>
	 * 
	 * @param namespace
	 *            命名空间
	 * @param tableName
	 *            表
	 * @param rowKey
	 *            row key
	 * @param clazz
	 *            映射对象
	 * @return <T>
	 * @since JDK 1.7
	 */
	public <T> T get(String namespace, String tableName, String rowKey, Class<? extends T> clazz) throws Exception;

	/**
	 * get:(获取数据). <br/>
	 * 
	 * @param namespace
	 *            命名空间
	 * @param tableName
	 *            表
	 * @param rowKey
	 *            row key
	 * @param columns
	 *            列
	 * @param clazz
	 *            映射对象
	 * @return <T>
	 * @since JDK 1.7
	 */
	public <T> T get(String namespace, String tableName, String rowKey, List<ColumnInfo> columns, Class<? extends T> clazz) throws Exception;

	/**
	 * get:(这里用一句话描述这个方法的作用). <br/>
	 * 
	 * @param tableName
	 *            表
	 * @param rowKey
	 *            row key
	 * @param columns
	 *            列
	 * @param filters
	 *            过滤器
	 * @param clazz
	 *            映射对象
	 * @return <T>
	 * @since JDK 1.7
	 */
	<T> T get(String tableName, String rowKey, List<ColumnInfo> columns, List<ColumnInfo> filters, Class<? extends T> clazz) throws Exception;

	/**
	 * 
	 * get:(这里用一句话描述这个方法的作用). <br/>
	 * 
	 * @param tableName
	 *            表
	 * @param rowKey
	 *            row key
	 * @param clazz
	 *            映射对象
	 * @return <T>
	 * @since JDK 1.7
	 */
	<T> T get(String tableName, String rowKey, Class<? extends T> clazz) throws Exception;

	/**
	 * get:(这里用一句话描述这个方法的作用). <br/>
	 * 
	 * @param tableName
	 *            表
	 * @param rowKey
	 *            row key
	 * @param columns
	 *            列
	 * @param clazz
	 *            映射对象
	 * @return <T>
	 * @since JDK 1.7
	 */
	<T> T get(String tableName, String rowKey, List<ColumnInfo> columns, Class<? extends T> clazz) throws Exception;

	/**
	 * get:(这里用一句话描述这个方法的作用). <br/>
	 * 
	 * @param tableName
	 *            表
	 * @param rowKey
	 *            row key
	 * @param column
	 *            列
	 * @return String
	 * @since JDK 1.7
	 */
	String getSingleColumnValue(String tableName, String rowKey, String column) throws Exception;

	/**
	 * get:(这里用一句话描述这个方法的作用). <br/>
	 * 
	 * @param tableName
	 *            表
	 * @param rowKey
	 *            row key
	 * @param column
	 *            列
	 * @param clazz
	 *            映射对象
	 * @return <T>
	 * @since JDK 1.7
	 */
	<T> T getSingleColumnValue(String tableName, String rowKey, String column, Class<? extends T> clazz) throws Exception;

	/**
	 * get:(读取某一列数据，默认列族). <br/>
	 * 
	 * @param namespace
	 *            命名空间
	 * @param tableName
	 *            表
	 * @param rowKey
	 *            row key
	 * @param column
	 *            列
	 * @param clazz
	 *            映射对象
	 * @return <T>
	 * @since JDK 1.7
	 */
	public <T> T getSingleColumnValueDefaultFamily(String namespace, String tableName, String rowKey, String column, Class<? extends T> clazz) throws Exception;

	/**
	 * get:(读取某一列数据，默认列族). <br/>
	 * 
	 * @param tableName
	 *            表
	 * @param family
	 *            列族
	 * @param rowKey
	 *            row key
	 * @param column
	 *            列
	 * @param clazz
	 *            映射对象
	 * @return <T>
	 * @since JDK 1.7
	 */
	public <T> T getSingleColumnValueDefaultNameSpace(String tableName, String rowKey, String family, String column, Class<? extends T> clazz) throws Exception;

	/**
	 * get:(读取某一列数据). <br/>
	 * 
	 * @param namespace
	 *            命名空间
	 * @param tableName
	 *            表
	 * @param family
	 *            列族
	 * @param rowKey
	 *            row key
	 * @param column
	 *            列
	 * @param clazz
	 *            映射对象
	 * @return <T>
	 * @since JDK 1.7
	 */
	public <T> T getSingleColumnValue(String namespace, String tableName, String rowKey, String family, String column, Class<? extends T> clazz) throws Exception;

	/**
	 * get:(这里用一句话描述这个方法的作用). <br/>
	 * 
	 * @param namespace
	 *            命名空间
	 * @param tableName
	 *            表
	 * @return List<String>
	 * @since JDK 1.7
	 */
	List<String> getRowKeys(String namespace, String tableName) throws Exception;

	/**
	 * get:(这里用一句话描述这个方法的作用). <br/>
	 * 
	 * @param tableName
	 *            表
	 * @return List<String>
	 * @since JDK 1.7
	 */
	List<String> getRowKeys(String tableName) throws Exception;

	List<String> getRowKeys(String tableName, String startRow, String endRow) throws Exception;

	List<String> getRowKeys(String namespace, String tableName, String startRow, String endRow) throws Exception;

	List<String> getRowKeys(String namespace, String tableName, String startRow, String endRow, Integer pageSize, String separate, Integer index) throws Exception;

	List<String> getRowKeys(String tableName, String startRow, String endRow, Integer pageSize, String separate, Integer index) throws Exception;

	List<String> getRowKeys(String namespace, String tableName, String startRow, String endRow, Integer pageSize, String separate) throws Exception;

	List<String> getRowKeys(String namespace, String tableName, String startRow, String endRow, Integer pageSize) throws Exception;

	List<String> getRowKeysByPrefix(String tableName, String prefix) throws Exception;

	List<String> getRowKeysByPrefix(String namespace, String tableName, String prefix) throws Exception;

	List<String> getRowKeysByPrefix(String namespace, String tableName, String start, String end, String prefix) throws Exception;

	List<ColumnInfo> getColumns(String namespace, String tableName, String rowKey, String columnFamily, List<ColumnInfo> columns, List<ColumnInfo> filters) throws Exception;

	List<ColumnInfo> getColumns(String namespace, String tableName, String rowKey, List<ColumnInfo> columns, List<ColumnInfo> filters) throws Exception;

	List<ColumnInfo> getColumns(String namespace, String tableName, String rowKey, String columnFamily) throws Exception;

	List<ColumnInfo> getColumns(String namespace, String tableName, String rowKey) throws Exception;

	<T> List<T> getList(String namespace, String tableName, List<String> rowKeys, Class<? extends T> clazz) throws Exception;

	<T> List<T> getList(String namespace, String tableName, List<String> rowKeys, List<ColumnInfo> columns, List<ColumnInfo> filters, Class<? extends T> clazz) throws Exception;

	<T> List<T> getList(String namespace, String tableName, Class<? extends T> clazz) throws Exception;

	<T> List<T> getList(String namespace, String tableName, List<ColumnInfo> columns, List<ColumnInfo> filters, Class<? extends T> clazz) throws Exception;

	<T> List<T> getList(String namespace, String tableName, List<ColumnInfo> columns, List<ColumnInfo> filters, String start, String end, Class<? extends T> clazz)
			throws Exception;

	/**
	 * 
	 * getPageList:(分页查找hbase数据). <br/>
	 *
	 * @param namespace
	 *            命名空间
	 * @param tableName
	 *            表名
	 * @param startRow
	 *            开始rowkey
	 * @param endRow
	 *            结束rowkey
	 * @param pageSize
	 *            每一页大小
	 * @param clazz
	 *            转换的对象
	 * @return List<T>
	 * @throws Exception
	 */
	<T> List<T> getPageList(String namespace, String tableName, String startRow, String endRow, Integer pageSize, Class<? extends T> clazz) throws Exception;

	List<ColumnInfo> getColumnsByPage(String namespace, String tableName, String rowKey, Integer pageNo, Integer pageSize) throws Exception;

	List<ColumnInfo> getColumnsByPage(String namespace, String tableName, String rowKey, Integer pageNo, Integer pageSize, List<ColumnInfo> columns, List<ColumnInfo> filters)
			throws Exception;

	<T> T getColumnObj(String namespace, String tableName, String rowKey, String column, Class<? extends T> clazz) throws Exception;

	<T> List<T> getColumnObjList(String namespace, String tableName, String rowKey, List<String> columns, Class<? extends T> clazz) throws Exception;

	<T> List<T> getPageColumnObjList(String namespace, String tableName, String rowKey, Integer pageNo, Integer pageSize, Class<? extends T> clazz) throws Exception;

	/**
	 * put:(新增数据). <br/>
	 * 
	 * @param namespace
	 *            命名空间
	 * @param tableName
	 *            待操作表名称
	 * @param family
	 *            列族集合
	 * @param objects
	 *            待插入数据对接集合
	 * @param isNotExistForCreate
	 *            true: 表不存在，则新建表;false:不会新建。
	 * @return true:添加成功;
	 * @throws Exception
	 */
	public <T> boolean put(String namespace, String tableName, String[] family, List<T> objects, boolean isNotExistForCreate) throws Exception;

	public <T> boolean put(String namespace, String tableName, List<T> objects, boolean isNotExistForCreate) throws Exception;

	/**
	 * put:(新增数据。 如果数据表不存在，则不会新建，插入数据失败。). <br/>
	 *
	 * @param tableName
	 *            待操作表名称
	 * @param objects
	 *            待插入数据
	 * @return 添加是否成功
	 * @throws Exception
	 */
	<T> boolean put(String namespace, String tableName, List<T> objects) throws Exception;

	<T> boolean put(String namespace, String tableName, T object) throws Exception;

	boolean put(String namespace, String tableName, String rowKey, String column, String value) throws Exception;

	/**
	 * 添加数据
	 * 
	 * @param tableName
	 *            表明
	 * @param rowKey
	 *            行键
	 * @param familyName
	 *            列簇
	 * @param columnName
	 *            列名
	 * @param value
	 *            值
	 * @return boolean true,插入成功；
	 */
	public boolean put(String namespace, String tableName, String rowKey, String familyName, String columnName, String value) throws Exception;

	boolean put(String namespace, String tableName, String rowKey, ColumnInfo columnInfo) throws Exception;

	boolean put(String namespace, String tableName, String rowKey, List<ColumnInfo> columnInfos) throws Exception;

	public boolean put(String namespace, String tableName, String rowKey, List<ColumnInfo> columnInfos, Long timestamp) throws Exception;

	boolean delete(String namespace, String tableName, String rowKey) throws Exception;

	boolean delete(String namespace, String tableName, String rowKey, List<ColumnInfo> list) throws Exception;

	boolean delete(String namespace, String tableName, String rowKey, ColumnInfo columnInfo) throws Exception;

	boolean delete(String namespace, String tableName, String rowKey, String column) throws Exception;

	long addCounter(String namespace, String tableName, String rowKey, String column, long num) throws Exception;

}
