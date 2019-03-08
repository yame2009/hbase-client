package com.tool.hbase.bean;

import org.apache.hadoop.hbase.filter.CompareFilter;

import com.tool.hbase.constants.HBaseConstant;

/**
 * ColumnInfo：封装最基础的单元格类，columnFamily，column和对应的value
 * 
 * <p>
 * the base entity contains columnFamily and column、value、op default value
 * String.class,others set customize valueClass used on limit the back columns
 * and filter values
 * </p>
 */
public class ColumnInfo {

	private String columnFamily;
	private String column;

	// 依据前文的设计思路，这里我们默认value为String类型，大多数情况下也应该这样做，如果有特殊的类型，如Long之类的，需指定valueClass的class
	private String value;

	// 比较器属性，可以设置这个值用于在HBase限制返回column和值过滤的时候传入，可取的值：EQUAL|NOT
	// EQUAL|GREATER等，我们这个类默认EQUAL。
	private CompareFilter.CompareOp compareOperator;

	private Class valueClass;

	public ColumnInfo() {
	}

	public ColumnInfo(String column) {
		this(HBaseConstant.DEFAULT_FAMILY, column, CompareFilter.CompareOp.EQUAL);
	}

	public ColumnInfo(String columnFamily, String column, CompareFilter.CompareOp compareOperator) {
		this.columnFamily = columnFamily;
		this.column = column;
		this.compareOperator = compareOperator;
	}

	public ColumnInfo(String columnFamily, String column, CompareFilter.CompareOp compareOperator, Class valueClass) {
		this(columnFamily, column, compareOperator);
		this.valueClass = valueClass;
	}

	public ColumnInfo(String column, String value) {
		this(HBaseConstant.DEFAULT_FAMILY, column, value, CompareFilter.CompareOp.EQUAL);
	}

	public ColumnInfo(String columnFamily, String column, String value) {
		this(columnFamily, column, value, CompareFilter.CompareOp.EQUAL);
	}

	public ColumnInfo(String columnFamily, String column, String value, CompareFilter.CompareOp compareOperator) {

		this(columnFamily, column, compareOperator);
		this.value = value;
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public CompareFilter.CompareOp getCompareOperator() {
		return compareOperator;
	}

	public void setCompareOperator(CompareFilter.CompareOp compareOperator) {
		this.compareOperator = compareOperator;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public Class getValueClass() {
		return valueClass;
	}

	public void setValueClass(Class valueClass) {
		this.valueClass = valueClass;
	}

	@Override
	public String toString() {
		return "ColumnInfo [columnFamily=" + columnFamily + ", column=" + column + ", value=" + value + ", compareOperator=" + compareOperator + ", valueClass=" + valueClass + "]";
	}

}
