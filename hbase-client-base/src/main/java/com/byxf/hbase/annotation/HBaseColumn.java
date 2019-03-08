package com.byxf.hbase.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>
 * used on the field associate to the column from the hbase
 * </p>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.FIELD })
public @interface HBaseColumn {

	// 列簇属性，用于指定特殊的列簇，项目里面有默认的全局family
	String family() default "";

	// 列属性，用于po属性和HBase对应column不匹配的情况，若一致无需指定
	String column();

	// 若po中某个字段不在HBase存在的话，需手动设置这个属性为false，但建议po类为纯净的po
	boolean exist() default true;
}
