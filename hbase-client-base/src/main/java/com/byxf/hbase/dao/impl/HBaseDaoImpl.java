package com.byxf.hbase.dao.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;
import javax.annotation.Resource;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import com.byxf.hbase.bean.ColumnInfo;
import com.byxf.hbase.config.HBaseFactoryBean;
import com.byxf.hbase.constants.HBaseConstant;
import com.byxf.hbase.dao.HBaseDao;
import com.byxf.hbase.dao.HBaseDdlDao;
import com.byxf.hbase.utils.HBaseAssistUtil;
import com.byxf.hbase.utils.JacksonUtil;
import com.google.common.collect.Lists;

/**
 * hbase 数据 DML 操作DAO
 */
@Service("hBaseDao")
public class HBaseDaoImpl implements HBaseDao {

	private Logger logger = LoggerFactory.getLogger(HBaseDaoImpl.class);

	@Resource
	private HBaseFactoryBean hBaseFactoryBean;

	@Resource
	private HBaseDdlDao hBaseDdlDao;

	private HConnection getConnection() {
		return hBaseFactoryBean.getDefaultConnection();
	}

	@Override
	public <T> T get(String namespace, String tableName, String rowKey, @Nullable List<ColumnInfo> columns, @Nullable List<ColumnInfo> filters, Class<? extends T> clazz)
			throws Exception {
		if (clazz == null || StringUtils.isBlank(rowKey) || StringUtils.isBlank(tableName)) {
			return null;
		}
		HTable hTable = null;
		T instance = null;
		try {
			hTable = hBaseDdlDao.getTable(namespace, tableName);
			Get get = new Get(rowKey.getBytes());
			HBaseAssistUtil.setColumnAndFilter(get, columns, filters);
			Result rs = hTable.get(get);
			if (!rs.isEmpty()) {
				instance = HBaseAssistUtil.parseObject(clazz, rs);
			}

		} catch (Exception e) {
			logger.error("get hbase data error", e);
			throw e;
		} finally {
			try {
				if (hTable != null) {
					hTable.close();
				}
			} catch (IOException e) {
				logger.error("hbase table close error", e);
			}
		}
		return instance;

	}

	@Override
	public <T> T get(String namespace, String tableName, String rowKey, Class<? extends T> clazz) throws Exception {
		return get(namespace, tableName, rowKey, null, null, clazz);
	}

	@Override
	public <T> T get(String namespace, String tableName, String rowKey, List<ColumnInfo> columns, Class<? extends T> clazz) throws Exception {
		return get(namespace, tableName, rowKey, columns, null, clazz);
	}

	@Override
	public <T> T get(String tableName, String rowKey, @Nullable List<ColumnInfo> columns, @Nullable List<ColumnInfo> filters, Class<? extends T> clazz) throws Exception {
		return get(null, tableName, rowKey, columns, filters, clazz);
	}

	@Override
	public <T> T get(String tableName, String rowKey, Class<? extends T> clazz) throws Exception {
		return get(null, tableName, rowKey, null, null, clazz);
	}

	@Override
	public <T> T get(String tableName, String rowKey, List<ColumnInfo> columns, Class<? extends T> clazz) throws Exception {
		return get(null, tableName, rowKey, columns, null, clazz);
	}

	@Override
	public String getSingleColumnValue(String tableName, String rowKey, String column) throws Exception {
		return getSingleColumnValue(null, tableName, rowKey, null, column, String.class);
	}

	@Override
	public <T> T getSingleColumnValue(String tableName, String rowKey, String column, Class<? extends T> clazz) throws Exception {
		return getSingleColumnValue(null, tableName, rowKey, null, column, clazz);
	}

	@Override
	public <T> T getSingleColumnValueDefaultFamily(String namespace, String tableName, String rowKey, String column, Class<? extends T> clazz) throws Exception {
		return getSingleColumnValue(namespace, tableName, rowKey, null, column, clazz);
	}

	@Override
	public <T> T getSingleColumnValueDefaultNameSpace(String tableName, String rowKey, String family, String column, Class<? extends T> clazz) throws Exception {
		return getSingleColumnValue(null, tableName, rowKey, family, column, clazz);
	}

	@Override
	public <T> T getSingleColumnValue(String namespace, String tableName, String rowKey, String family, String column, Class<? extends T> clazz) throws Exception {
		if (StringUtils.isBlank(column) || StringUtils.isBlank(rowKey)) {
			logger.error("hbase column or rowKey is null");
			return null;
		}

		HTable hTable = null;
		T t = null;
		try {
			hTable = hBaseDdlDao.getTable(namespace, tableName);
			Get get = new Get(Bytes.toBytes(rowKey));

			if (StringUtils.isBlank(family)) {
				family = HBaseConstant.DEFAULT_FAMILY;
			}
			get.addColumn(family.getBytes(), column.getBytes());
			Result result = hTable.get(get);
			for (Cell cell : result.rawCells()) {
				t = HBaseAssistUtil.convert(clazz, CellUtil.cloneValue(cell));
			}
		} catch (Exception e) {
			logger.error("get hbase data error", e);
			throw e;
		} finally {
			try {
				hTable.close();
			} catch (IOException e) {
				logger.error("close hbase table error", e);
				throw e;
			}
		}
		return t;
	}

	/**
	 * get:(这里用一句话描述这个方法的作用). <br/>
	 * 
	 * @param tableName
	 *            表
	 * @return List<String>
	 * @since JDK 1.7
	 */
	@Override
	public List<String> getRowKeys(String tableName) throws Exception {
		return getRowKeys(null, tableName, null, null, null, null, null);
	}

	@Override
	public List<String> getRowKeys(String tableName, String startRow, String endRow) throws Exception {
		return getRowKeys(null, tableName, startRow, endRow, null, null, null);
	}

	@Override
	public List<String> getRowKeys(String namespace, String tableName) throws Exception {
		return getRowKeys(namespace, tableName, null, null, null, null, null);
	}

	@Override
	public List<String> getRowKeys(String namespace, String tableName, String start, String end) throws Exception {
		return getRowKeysByPrefix(namespace, tableName, start, end, null);
	}

	@Override
	public List<String> getRowKeys(String tableName, String startRow, String endRow, Integer pageSize, String separate, Integer index) throws Exception {
		return getRowKeys(null, tableName, startRow, endRow, pageSize, separate, index);
	}

	@Override
	public List<String> getRowKeys(String namespace, String tableName, String startRow, String endRow, Integer pageSize, String separate, Integer index)
			throws Exception {

		List<String> rowKeys = new ArrayList<>();
		HTable hTable = null;
		ResultScanner scanner = null;
		try {
			hTable = hBaseDdlDao.getTable(namespace, tableName);
			Scan scan = new Scan();
			FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			if (StringUtils.isNotBlank(startRow)) {
				scan.setStartRow(startRow.getBytes());
			}
			if (StringUtils.isNotBlank(endRow)) {
				scan.setStopRow(endRow.getBytes());
			}

			Filter kof = new KeyOnlyFilter();
			filterList.addFilter(kof);
			scan.setFilter(filterList);
			scanner = hTable.getScanner(scan);
			for (Result result : scanner) {
				if (!result.isEmpty()) {
					String rowKey = new String(result.getRow());
					if (StringUtils.isNotBlank(separate)) {
						rowKeys.add(rowKey.split(separate)[index]);
					} else {
						rowKeys.add(rowKey);
					}
				}
			}
		} catch (Exception e) {
			logger.error("get hbase data error", e);
			throw e;
		} finally {
			try {
				if (scanner != null) {
					scanner.close();
				}
				if (hTable != null) {
					hTable.close();
				}
			} catch (IOException e) {
				logger.error("close hbase table error", e);
				throw e;
			}
		}
		return rowKeys;
	}

	@Override
	public List<String> getRowKeys(String namespace, String tableName, String startRow, String endRow, Integer pageSize, String separate) throws Exception {
		return getRowKeys(namespace, tableName, startRow, endRow, pageSize, separate, 1);
	}

	@Override
	public List<String> getRowKeys(String namespace, String tableName, String startRow, String endRow, Integer pageSize) throws Exception {
		return getRowKeys(namespace, tableName, startRow, endRow, pageSize, null, null);
	}

	@Override
	public List<String> getRowKeysByPrefix(String tableName, String prefix) throws Exception {
		return getRowKeysByPrefix(null, tableName, null, null, prefix);
	}

	@Override
	public List<String> getRowKeysByPrefix(String namespace, String tableName, String prefix) throws Exception {
		return getRowKeysByPrefix(namespace, tableName, null, null, prefix);
	}

	@Override
	public List<String> getRowKeysByPrefix(String namespace, String tableName, String start, String end, String prefix) throws Exception {
		List<String> rowKeys = new ArrayList<>();
		HTable hTable = null;
		ResultScanner scanner = null;
		try {
			hTable = hBaseDdlDao.getTable(namespace, tableName);
			Scan scan = new Scan();
			if (StringUtils.isNotBlank(start)) {
				scan.setStartRow(start.getBytes());
			}
			
			if (StringUtils.isNotBlank(end)) {
				scan.setStopRow(end.getBytes());
			}
			
			FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			Filter kof = new KeyOnlyFilter();
			if (StringUtils.isNotBlank(prefix)) {
				Filter prefixFilter = new PrefixFilter(prefix.getBytes());
				filterList.addFilter(prefixFilter);
			}
			
			filterList.addFilter(kof);
			scan.setFilter(filterList);
			
			scanner = hTable.getScanner(scan);
			
			for (Result result : scanner) {
				if (!result.isEmpty()) {
					rowKeys.add(new String(result.getRow()));
				}
			}
			
		} catch (Exception e) {
			logger.error("get hbase data error", e);
			throw e;
		} finally {
			try {
				if (scanner != null) {
					scanner.close();
				}
				if (hTable != null) {
					hTable.close();
				}
			} catch (IOException e) {
				logger.error("close hbase table error", e);
				throw e;
			}
		}
		return rowKeys;
	}

	@Override
	public List<ColumnInfo> getColumns(String namespace, String tableName, String rowKey, String columnFamily, List<ColumnInfo> columns, List<ColumnInfo> filters)
			throws Exception {
		HTable hTable = null;
		List<ColumnInfo> dataList = new ArrayList<>();
		try {
			hTable = hBaseDdlDao.getTable(namespace, tableName);
			Get get = new Get(Bytes.toBytes(rowKey));
			get.addFamily(columnFamily.getBytes());
			HBaseAssistUtil.setColumnAndFilter(get, columns, filters);
			Result result = hTable.get(get);

			for (Cell cell : result.rawCells()) {
				String column = new String(CellUtil.cloneQualifier(cell), "utf-8");
				String value = new String(CellUtil.cloneValue(cell), "utf-8");
				ColumnInfo bean = new ColumnInfo();
				bean.setColumn(column);
				bean.setValue(value);
				dataList.add(bean);
			}
		} catch (Exception e) {
			logger.error("get hbase data error", e);
			throw e;
		} finally {
			try {
				hTable.close();
			} catch (IOException e) {
				logger.error("close hbase table error", e);
				throw e;
			}
		}
		return dataList;
	}

	@Override
	public List<ColumnInfo> getColumns(String namespace, String tableName, String rowKey, List<ColumnInfo> columns, List<ColumnInfo> filters) throws Exception {
		return getColumns(namespace, tableName, rowKey, HBaseConstant.DEFAULT_FAMILY, columns, null);
	}

	@Override
	public List<ColumnInfo> getColumns(String namespace, String tableName, String rowKey, String columnFamily) throws Exception {
		return getColumns(namespace, tableName, rowKey, columnFamily, null, null);
	}

	@Override
	public List<ColumnInfo> getColumns(String namespace, String tableName, String rowKey) throws Exception {
		return getColumns(namespace, tableName, rowKey, HBaseConstant.DEFAULT_FAMILY);
	}

	@Override
	public <T> List<T> getList(String namespace, String tableName, List<String> rowKeys, Class<? extends T> clazz) throws Exception {
		return getList(namespace, tableName, rowKeys, null, null, clazz);
	}

	@Override
	public <T> List<T> getList(String namespace, String tableName, List<String> rowKeys, List<ColumnInfo> columns, List<ColumnInfo> filters, Class<? extends T> clazz)
			throws Exception {
		if (clazz == null || rowKeys == null || rowKeys.size() == 0) {
			return null;
		}
		HTable hTable = null;
		List<T> resultList = new ArrayList<>();
		try {
			ArrayList<Get> getlist = new ArrayList<>();
			hTable = hBaseDdlDao.getTable(namespace, tableName);

			if (rowKeys != null && rowKeys.size() > 0) {
				for (String rowKey : rowKeys) {
					if (StringUtils.isNotBlank(rowKey)) {
						Get get = new Get(rowKey.getBytes());
						HBaseAssistUtil.setColumnAndFilter(get, columns, filters);
						getlist.add(get);
					}
				}
			}

			Result[] resultsset = hTable.get(getlist);
			for (Result results : resultsset) {
				if (!results.isEmpty()) {
					T instance = HBaseAssistUtil.parseObject(clazz, results);
					resultList.add(instance);
				} else {
					continue;
				}
			}

		} catch (Exception e) {
			logger.error("get hbase data error", e);
			throw e;
		} finally {
			try {
				if (hTable != null) {
					hTable.close();
				}
			} catch (IOException e) {
				logger.error("close hbase table error", e);
				throw e;
			}
		}
		return resultList;
	}

	@Override
	public <T> List<T> getList(String namespace, String tableName, Class<? extends T> clazz) throws Exception {
		return getList(namespace, tableName, null, null, clazz);
	}

	@Override
	public <T> List<T> getList(String namespace, String tableName, @Nullable List<ColumnInfo> columns, @Nullable List<ColumnInfo> filters, Class<? extends T> clazz)
			throws Exception {
		return getList(namespace, tableName, columns, filters, null, null, clazz);
	}

	@Override
	public <T> List<T> getList(String namespace, String tableName, List<ColumnInfo> columns, List<ColumnInfo> filters, String start, String end, Class<? extends T> clazz)
			throws Exception {
		if (clazz == null) {
			return null;
		}
		HTable hTable = null;
		List<T> list = new ArrayList<>();
		try {
			hTable = hBaseDdlDao.getTable(namespace, tableName);
			Scan scan = new Scan();
			if (StringUtils.isNotBlank(start)) {
				scan.setStartRow(start.getBytes());
			}
			if (StringUtils.isNotBlank(end)) {
				scan.setStopRow(end.getBytes());
			}
			HBaseAssistUtil.setColumnAndFilter(scan, columns, filters);
			ResultScanner scanner = hTable.getScanner(scan);
			for (Result rs : scanner) {
				if (!rs.isEmpty()) {
					T instance = HBaseAssistUtil.parseObject(clazz, rs);
					list.add(instance);
				}
			}
		} catch (Exception e) {
			logger.error("get hbase data error", e);
			throw e;
		} finally {
			try {
				if (hTable != null) {
					hTable.close();
				}
			} catch (IOException e) {
				logger.error("close hbase table error", e);
				throw e;
			}
		}
		return list;
	}

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
	@Override
	public <T> List<T> getPageList(String namespace, String tableName, String startRow, String endRow, Integer pageSize, Class<? extends T> clazz) throws Exception {
		if (clazz == null) {
			return null;
		}
		HTable hTable = null;
		List<T> list = new ArrayList<>();
		try {
			hTable = hBaseDdlDao.getTable(namespace, tableName);
			Scan scan = new Scan();
			if (StringUtils.isNotBlank(startRow)) {
				scan.setStartRow(startRow.getBytes());
			}
			if (StringUtils.isNotBlank(endRow)) {
				scan.setStopRow(endRow.getBytes());
			}

			// 客户端每次 rpc fetch 的行数)
			// scan.setBatch(pageSize);
			// 客户端缓存的最大字节数
			// scan.setMaxResultSize(pageSize);
			// 客户端每次获取的列数
			// scan.setBatch(pageSize);
			PageFilter pageFilter = new PageFilter(pageSize);
			scan.setFilter(pageFilter);

			ResultScanner scanner = hTable.getScanner(scan);
			for (Result rs : scanner) {
				if (!rs.isEmpty()) {
					T instance = HBaseAssistUtil.parseObject(clazz, rs);
					list.add(instance);
				}
			}
		} catch (Exception e) {
			logger.error("get hbase data error", e);
			throw e;
		} finally {
			try {
				if (hTable != null) {
					hTable.close();
				}
			} catch (IOException e) {
				logger.error("close hbase table error", e);
				throw e;
			}
		}
		return list;
	}

	@Override
	public List<ColumnInfo> getColumnsByPage(String namespace, String tableName, String rowkey, Integer pageNo, Integer pageSize) throws Exception {
		return getColumnsByPage(namespace, tableName, rowkey, pageNo, pageSize, null, null);
	}

	@Override
	public List<ColumnInfo> getColumnsByPage(String namespace, String tableName, String rowkey, Integer pageNo, Integer pageSize, List<ColumnInfo> columns,
			List<ColumnInfo> filters)
			throws Exception {
		HTable hTable = null;
		List<ColumnInfo> dataList = new ArrayList<>();
		try {
			hTable = hBaseDdlDao.getTable(namespace, tableName);
			Get get = new Get(Bytes.toBytes(rowkey));
			HBaseAssistUtil.setColumnAndFilter(get, columns, filters);
			if (pageNo != null && pageNo != 0 && pageSize != 0 && pageSize != null) {
				get.setMaxResultsPerColumnFamily(pageSize);
				get.setRowOffsetPerColumnFamily((pageNo - 1) * pageSize);
			}
			Result result = hTable.get(get);

			for (Cell cell : result.rawCells()) {
				String column = new String(CellUtil.cloneQualifier(cell), "utf-8");
				String value = new String(CellUtil.cloneValue(cell), "utf-8");
				ColumnInfo bean = new ColumnInfo();
				bean.setColumn(column);
				bean.setValue(value);
				dataList.add(bean);
			}
		} catch (Exception e) {
			logger.error("get hbase data error", e);
			throw e;
		} finally {
			try {
				hTable.close();
			} catch (IOException e) {
				logger.error("close hbase table error", e);
				throw e;
			}
		}
		return dataList;
	}

	@Override
	public <T> T getColumnObj(String namespace, String tableName, String rowKey, String column, Class<? extends T> clazz) throws Exception {
		if (StringUtils.isBlank(column)) {
			return null;
		}
		List<T> list = getColumnObjList(namespace, tableName, rowKey, Lists.<String>newArrayList(column), clazz);
		if (list != null && list.size() > 0) {
			return list.get(0);
		}
		return null;
	}

	@Override
	public <T> List<T> getColumnObjList(String namespace, String tableName, String rowKey, List<String> columns, Class<? extends T> clazz) throws Exception {
		HTable hTable = null;
		List<T> dataList = new ArrayList<>();
		try {
			hTable = hBaseDdlDao.getTable(namespace, tableName);
			Get get = new Get(Bytes.toBytes(rowKey));
			if (columns != null && columns.size() > 0) {
				for (String column : columns) {
					get.addColumn(HBaseConstant.DEFAULT_FAMILY.getBytes(), column.getBytes());
				}
			}
			Result result = hTable.get(get);

			for (Cell cell : result.rawCells()) {
				String value = new String(CellUtil.cloneValue(cell), "utf-8");
				dataList.add((T) JacksonUtil.jsonToBean(value, clazz));
			}
		} catch (Exception e) {
			logger.error("get hbase data error", e);
			throw e;
		} finally {
			try {
				hTable.close();
			} catch (IOException e) {
				logger.error("close hbase table error", e);
				throw e;
			}
		}
		return dataList;
	}

	@Override
	public <T> List<T> getPageColumnObjList(String namespace, String tableName, String rowKey, Integer pageNo, Integer pageSize, Class<? extends T> clazz) throws Exception {
		HTable hTable = null;
		List<T> dataList = new ArrayList<>();
		try {
			hTable = hBaseDdlDao.getTable(namespace, tableName);
			Get get = new Get(Bytes.toBytes(rowKey));
			if (pageNo != null && pageNo != 0 && pageSize != 0 && pageSize != null) {
				get.setMaxResultsPerColumnFamily(pageSize);
				get.setRowOffsetPerColumnFamily((pageNo - 1) * pageSize);
			}
			Result result = hTable.get(get);

			for (Cell cell : result.rawCells()) {
				String value = new String(CellUtil.cloneValue(cell), "utf-8");
				dataList.add((T) JacksonUtil.jsonToBean(value, clazz));
			}
		} catch (Exception e) {
			logger.error("get hbase data error", e);
			throw e;
		} finally {
			try {
				hTable.close();
			} catch (IOException e) {
				logger.error("close hbase table error", e);
				throw e;
			}
		}
		return dataList;
	}

	@Override
	public <T> boolean put(String namespace, String tableName, List<T> objects) throws Exception {
		return put(namespace, tableName, null, objects, false);
	}

	public <T> boolean put(String namespace, String tableName, List<T> objects, boolean isNotExistForCreate) throws Exception {
		return put(namespace, tableName, null, objects, isNotExistForCreate);
	}

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
	@Override
	public <T> boolean put(String namespace, String tableName, String[] family, List<T> objects, boolean isNotExistForCreate) throws Exception {

		// 如果不存在表
		boolean existTable = hBaseDdlDao.existTable(namespace, tableName, family, isNotExistForCreate);
		if (!existTable) {
			logger.error("put hbase data fail,tableName={} is not exist ", tableName);
			return false;
		}

		boolean isSucess = false;
		HTable hTable = null;
		try {
			List<Put> puts = HBaseAssistUtil.putObjectList(objects);

			hTable = hBaseDdlDao.getTable(namespace, tableName);
			if (puts != null && puts.size() > 0) {
				hTable.put(puts);
				hTable.flushCommits();
			}
			isSucess = true;
		} catch (Exception e) {
			logger.error("put hbase data error", e);
			throw e;
		} finally {
			try {
				if (hTable != null) {
					hTable.close();
				}
			} catch (IOException e) {
				logger.error("close hbase table error", e);
				throw e;
			}
		}
		return isSucess;
	}

	@Override
	public <T> boolean put(String namespace, String tableName, T object) throws Exception {
		Assert.notNull(object, "error,obj is null ");
		return put(namespace, tableName, Lists.newArrayList(object));
	}

	@Override
	public boolean put(String namespace, String tableName, String rowKey, String column, String value) throws Exception {
		ColumnInfo columnInfo = new ColumnInfo(HBaseConstant.DEFAULT_FAMILY, column, value);
		return put(namespace, tableName, rowKey, columnInfo);
	}

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
	@Override
	public boolean put(String namespace, String tableName, String rowKey, String familyName, String columnName, String value) throws Exception {
		// 转化为表名
		ColumnInfo columnInfo = new ColumnInfo(familyName, columnName, value);
		return put(namespace, tableName, rowKey, columnInfo);

	}

	@Override
	public boolean put(String namespace, String tableName, String rowKey, ColumnInfo columnInfo) throws Exception {
		Assert.isTrue(columnInfo != null, "column info should have value");
		return put(namespace, tableName, rowKey, Lists.<ColumnInfo>newArrayList(columnInfo));
	}

	@Override
	public boolean put(String namespace, String tableName, String rowKey, List<ColumnInfo> columnInfos) throws Exception {
		return put(namespace, tableName, rowKey, columnInfos, null);
	}

	@Override
	public boolean put(String namespace, String tableName, String rowKey, List<ColumnInfo> columnInfos, Long timestamp) throws Exception {
		Assert.isTrue(columnInfos != null && columnInfos.size() > 0, "column info should have value");
		HTable hTable = null;
		boolean isSuccess = false;
		try {
			hTable = hBaseDdlDao.getTable(namespace, tableName);
			Put put = new Put(rowKey.getBytes());
			for (ColumnInfo columnInfo : columnInfos) {
				if(timestamp == null) {
					put.add(columnInfo.getColumnFamily().getBytes(), columnInfo.getColumn().getBytes(), HBaseAssistUtil.getValueBytes(columnInfo.getValue(), columnInfo.getValueClass()));
				}
				else{
					put.addImmutable(columnInfo.getColumnFamily().getBytes(), columnInfo.getColumn().getBytes(), timestamp,
							HBaseAssistUtil.getValueBytes(columnInfo.getValue(), columnInfo.getValueClass()));
				}
				
			}
			hTable.put(put);
			hTable.flushCommits();

			isSuccess = true;
		} catch (Exception e) {
			logger.error("put hbase data error", e);
			throw e;
		} finally {
			try {
				if (hTable != null) {
					hTable.close();
				}
			} catch (IOException e) {
				logger.error("close hbase table error", e);
				throw e;
			}
		}
		return isSuccess;
	}

	@Override
	public boolean delete(String namespace, String tableName, String rowKey) throws Exception {
		Assert.notNull(rowKey, "row key is null");
		HTable hTable = null;
		boolean isSuccess = false;
		try {
			hTable = hBaseDdlDao.getTable(namespace, tableName);
			Delete del = new Delete(rowKey.getBytes());
			hTable.delete(del);
			hTable.flushCommits();
			isSuccess = true;
		} catch (Exception e) {
			logger.error("delete hbase data error", e);
			throw e;
		} finally {
			try {
				if (hTable != null) {
					hTable.close();
				}
			} catch (IOException e) {
				logger.error("close hbase table error", e);
				throw e;
			}
		}
		return isSuccess;
	}

	@Override
	public boolean delete(String namespace, String tableName, String rowKey, List<ColumnInfo> columnInfos) throws Exception {
		Assert.isTrue(columnInfos != null && columnInfos.size() > 0, "column info should have value");
		HTable hTable = null;
		boolean isSuccess = false;
		try {
			hTable = hBaseDdlDao.getTable(namespace, tableName);
			Delete del = new Delete(rowKey.getBytes());
			for (ColumnInfo columnInfo : columnInfos) {
				String defaultFamily = HBaseConstant.DEFAULT_FAMILY;
				if (StringUtils.isNotBlank(columnInfo.getColumnFamily())) {
					defaultFamily = columnInfo.getColumnFamily();
				}
				del.deleteColumns(defaultFamily.getBytes(), columnInfo.getColumn().getBytes());
			}
			hTable.delete(del);
			hTable.flushCommits();
			isSuccess = true;
		} catch (Exception e) {
			logger.error("delete hbase data error", e);
			throw e;
		} finally {
			try {
				if (hTable != null) {
					hTable.close();
				}
			} catch (IOException e) {
				logger.error("close hbase table error", e);
				throw e;
			}
		}
		return isSuccess;
	}

	@Override
	public boolean delete(String namespace, String tableName, String rowKey, ColumnInfo columnInfo) throws Exception {
		Assert.notNull(columnInfo, "error,obj is null ");
		return delete(namespace, tableName, rowKey, Lists.<ColumnInfo>newArrayList(columnInfo));
	}

	@Override
	public boolean delete(String namespace, String tableName, String rowKey, String column) throws Exception {
		ColumnInfo columnInfo = new ColumnInfo(column);
		return delete(namespace, tableName, rowKey, columnInfo);
	}

	@Override
	public long addCounter(String namespace, String tableName, String rowKey, String column, long num) throws Exception {
		HTable hTable = null;
		long result = -1;
		try {
			hTable = hBaseDdlDao.getTable(namespace, tableName);
			result = hTable.incrementColumnValue(rowKey.getBytes(), HBaseConstant.DEFAULT_FAMILY.getBytes(), column.getBytes(), num);
		} catch (Exception e) {
			logger.error("add hbase counter error", e);
			throw e;
		} finally {
			if (hTable != null) {
				try {
					hTable.close();
				} catch (Exception e) {
					logger.error("close hbase table error", e);
					throw e;
				}
			}
		}
		return result;
	}


}
