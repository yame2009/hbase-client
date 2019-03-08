package com.tool.hbase.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * field注解，用于注解po中的rowKey，若po中的属性不为rowkey值的话，需手动指定这个注解，否则将会默认field为rowkey
 * <p>
 * used on the field associate to the rowkey from the hbase
 * </p>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface RowKey {
}
