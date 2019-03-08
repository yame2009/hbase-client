package com.tool.hbase.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;

/**
 * 重写PropertyPlaceholderConfigurer：.通常我们使用spring的PropertyPlaceholderConfigurer类来读取配置信息.
 * date: 2018年11月30日 上午3:05:40 <br/>
 *
 * @author huangbing
 * @version
 * @since JDK 1.7
 */
public class PropertyPlaceholder extends PropertyPlaceholderConfigurer {

	private static Map<String, String> propertyMap;

	@Override
	protected void processProperties(ConfigurableListableBeanFactory beanFactoryToProcess, Properties props) throws BeansException {
		super.processProperties(beanFactoryToProcess, props);
		propertyMap = new HashMap<String, String>();
		for (Object key : props.keySet()) {
			String keyStr = key.toString();
			String value = props.getProperty(keyStr);
			propertyMap.put(keyStr, value);
		}
	}

	// static method for accessing context properties
	public static String getProperty(String name) {
		return propertyMap.get(name);
	}

	public static Map<String, String> getProperties() {
		return propertyMap;
	}
}
