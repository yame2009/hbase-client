package com.byxf.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.byxf.hbase.bean.ColumnInfo;
import com.byxf.hbase.config.HBaseFactoryBean;
import com.byxf.hbase.constants.HBaseConstant;
import com.byxf.hbase.dao.HBaseDao;
import com.byxf.hbase.dao.HBaseDdlDao;
import com.google.common.collect.Lists;

/**
 */
public class HBaseDaoTest extends BaseSpringTest {

	@Autowired
	private HBaseDao hBaseDao;

	@Resource
	private HBaseDdlDao hBaseDdlDao;

	@Resource
	private HBaseFactoryBean hBaseFactoryBean;

	@Test
	public void testGetRowKeys() {

        List<String> test1 = null;
        try {
			// test1 = hBaseDao.getRowKeys("rms_dev_ns", "risk_event_credit");//
			test1 = hBaseDao.getRowKeys(null, "test");
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(test1);
	}

	@Test
	public void testRowKeysPage() {
		try {
			System.out.println(
					hBaseDao.getRowKeys("rms_dev_ns", "risk_event_credit", "0_13403000003_301_1540566506526_creditAward", "0_13403000003_301_1540567301891_creditAward", 10, "_"));
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println();
	}

	@Test
	public void testCreateTable() {
		try {
			System.out.println(hBaseDdlDao.createTable("hb", "mop_articles_desc"));
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println();
	}

	@Test
	public void testNoAnnotationPut() {

		List<ArticleBean> articleBeans = new ArrayList<>();
		// articleBeans.add(new ArticleBean("11", "dzh", "测试忽略"));
		// articleBeans.add(new ArticleBean("22", "dzh", 23, "忽略标题", "不忽略中文"));
		// articleBeans.add(new ArticleBean("33", "dzh", 33, "忽略e标题",
		// "不忽略中sd文"));
		// articleBeans.add(new ArticleBean("55", "dzh", 55, "忽略3标题",
		// "不忽略中sd文"));
		ArticleBean e1 = new ArticleBean("77", "dzh", 77, "忽略77标题", "不忽略中sd文");
		e1.setStatus((byte) -1);
		articleBeans.add(e1);
		ArticleBean e2 = new ArticleBean("88", "dzh88", 88, "忽略88标题", "不忽略中sd文");
		e2.setStatus((byte) 1);
		articleBeans.add(e2);

		ArticleBean e3 = new ArticleBean("99", "dzh88", 99, "忽略99标题", "不忽略中sd文");
		e3.setStatus((byte) 0);
		articleBeans.add(e3);
		try {
			System.out.println(hBaseDao.put("hb", "mop_articles_desc", null, articleBeans, true));
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println();

		List<String> test1 = null;
		try {
			test1 = hBaseDao.getRowKeys("hb", "mop_articles_desc");//
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(test1);
	}

	@Test
	public void testPut1() {

		try {
			hBaseDdlDao.dropTable("hb", "mop_articles_desc23");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		List<ArticleBean> articleBeans = new ArrayList<>();
		ArticleBean e1 = new ArticleBean("77", "dzh", 77, "忽略77标题", "不忽略中sd文");
		e1.setStatus((byte) -1);
		articleBeans.add(e1);
		ArticleBean e2 = new ArticleBean("88", "dzh88", 88, "忽略88标题", "不忽略中sd文");
		e2.setStatus((byte) 1);
		articleBeans.add(e2);

		ArticleBean e3 = new ArticleBean("99", "dzh88", 99, "忽略99标题", "不忽略中sd文");
		e3.setStatus((byte) 0);
		articleBeans.add(e3);
		try {
			String[] family = { "content" };
			System.out.println(hBaseDao.put("hb", "mop_articles_desc23", family, articleBeans, true));
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println();

		List<String> test1 = null;
		try {
			test1 = hBaseDao.getRowKeys("hb", "mop_articles_desc");//
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(test1);
	}

	@Test
	public void testPut2() {

		try {
			hBaseDdlDao.dropTable("example", "defaultBeanExample");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		List<DefaultBeanExample> articleBeans = new ArrayList<>();
		DefaultBeanExample e1 = new DefaultBeanExample("77", "dzh", 77, "忽略77标题", "不忽略中sd文");
		e1.setStatus((byte) -1);
		articleBeans.add(e1);
		DefaultBeanExample e2 = new DefaultBeanExample("88", "dzh88", 88, "忽略88标题", "不忽略中sd文");
		e2.setStatus((byte) 1);
		articleBeans.add(e2);

		DefaultBeanExample e3 = new DefaultBeanExample("99", "dzh88", 99, "忽略99标题", "不忽略中sd文");
		e3.setStatus((byte) 0);
		articleBeans.add(e3);
		try {
			System.out.println(hBaseDao.put("example", "defaultBeanExample", null, articleBeans, true));
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println();

		List<String> test1 = null;
		try {
			test1 = hBaseDao.getRowKeys("example", "defaultBeanExample");//
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(test1);
	}

	@Test
	public void testHBaseRowKeyPrefix() throws IOException {

		HTable hTable = hBaseDdlDao.getTable("hb", "mop_articles_desc");
		Scan scan = new Scan();
		scan.setStartRow("11".getBytes());
		scan.setStopRow("66".getBytes());
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		Filter kof = new KeyOnlyFilter();
		filterList.addFilter(kof);
		scan.setFilter(filterList);

		ResultScanner scanner = hTable.getScanner(scan);

		for (Result result : scanner) {
			// 判断结果是否为空,是的话则跳过
			if (!result.isEmpty()) {
				System.out.println(new String(result.getRow()));
			}
		}
	}
	
	@Test
	public void testGetList2() {

		try {
			List<String> rowKeys = hBaseDao.getRowKeys("hb", "mop_articles_desc");
			List<ArticleBean> list = hBaseDao.getList("hb", "mop_articles_desc", rowKeys, ArticleBean.class);
			System.out.println(list);

			List<ArticleBean> list2 = hBaseDao.getList("hb", "mop_articles_desc", Lists.newArrayList("11", "22"), ArticleBean.class);
			System.out.println(list2);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

	@Test
	public void testGetPageList() {
		try {
			List<ArticleBean> list = hBaseDao.getPageList("hb", "mop_articles_desc", "11", "99", 2, ArticleBean.class);
			System.out.println(list);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testGetListFilter() {
		ColumnInfo columnInfo = new ColumnInfo();
		columnInfo.setColumnFamily(HBaseConstant.DEFAULT_FAMILY);
		columnInfo.setColumn("status");
		columnInfo.setValue("-1");
		columnInfo.setCompareOperator(CompareFilter.CompareOp.GREATER_OR_EQUAL);
		List<ColumnInfo> columnInfoList = new ArrayList<>();
		columnInfoList.add(columnInfo);
        try {
			List<ArticleBean> list = hBaseDao.getList("hb", "mop_articles_desc", Lists.newArrayList("55", "66", "77", "88", "99"), null, columnInfoList, ArticleBean.class);
			System.out.println(list);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

	@Test
	public void testNoAnnotationGet() {

		ArticleBean x = null;
		try {
			x = hBaseDao.get("hb:mop_articles_desc", "88", ArticleBean.class);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(x);
	}



	@Test
	public void testFilterLongValue() {

		List<ColumnInfo> filters = new ArrayList<>();
		ColumnInfo columnInfo = new ColumnInfo();
		columnInfo.setColumn("publishtime");
		columnInfo.setValue("1543824197403");
		columnInfo.setCompareOperator(CompareFilter.CompareOp.NOT_EQUAL);
		// 如果是long类型的值过滤 需添加class标识，否则默认为string的字节数组过滤
		columnInfo.setValueClass(Long.class);
		filters.add(columnInfo);

		ArticleBean x = null;
		try {
			x = hBaseDao.get("hb", "mop_articles_desc", "11", null, filters, ArticleBean.class);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(x);

	}

	@Test
	public void testColumns() {
		try {
			List<ColumnInfo> columns = hBaseDao.getColumns("hb", "mop_articles_desc", "11");
			System.out.println(columns);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Test
	public void testgetColumnsByPage() {
		try {
			List<ColumnInfo> columns = hBaseDao.getColumnsByPage("hb", "mop_articles_desc", "66", 1, 2);
			System.out.println(columns);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Test
	public void testDelete() {
		try {
			List<String> rowKeys = hBaseDao.getRowKeys("hb", "mop_articles_desc");
			List<ArticleBean> list = hBaseDao.getList("hb", "mop_articles_desc", rowKeys, ArticleBean.class);
			System.out.println(list);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		ColumnInfo columnInfo = new ColumnInfo();
		columnInfo.setColumn("articleid");
		columnInfo.setValue("33");

		try {
			boolean columns = hBaseDao.delete("hb", "mop_articles_desc", "33", columnInfo); // 按条件删除
			// boolean columns = hBaseDao.delete("hb", "mop_articles_desc",
			// "33"); // 删除多条
			System.out.println(columns);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Test
	public void testDeleteTable() {
		try {
			hBaseDdlDao.dropTable("hb", "mop_articles_desc23");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
