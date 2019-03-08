package com.byxf.hbase.utils;

/**
 * @包路径 com.byxf.hbase.utils
 * @创建人 huangbing
 * @创建时间 2018/11/21
 * @描述：
 */

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Jackson Util
 * 
 * @create 2018-11-21 19:10
 * @since 1.0.0
 **/
public class JacksonUtil {

	private static final ObjectMapper MAPPER = new ObjectMapper();
	private static final JsonFactory JSONFACTORY;

	public static String beanToJson(Object o) throws JsonParseException {
		StringWriter sw = new StringWriter();
		JsonGenerator jsonGenerator = null;

		String var3;
		try {
			jsonGenerator = JSONFACTORY.createJsonGenerator(sw);
			MAPPER.writeValue(jsonGenerator, o);
			var3 = sw.toString();
		} catch (Exception var12) {
			throw new RuntimeException("转换Java Bean 为 json错误", var12);
		} finally {
			if (jsonGenerator != null) {
				try {
					jsonGenerator.close();
				} catch (Exception var11) {
					;
				}
			}

		}

		return var3;
	}

	public static Object jsonToBean(String json, Class clazz) throws JsonParseException {
		try {
			return MAPPER.readValue(json, clazz);
		} catch (Exception var3) {
			throw new RuntimeException(var3 + "json 转 javabean错误");
		}
	}

	public static Map<String, Object> beanToMap(Object o) throws JsonParseException {
		try {
			return (Map) MAPPER.readValue(beanToJson(o), HashMap.class);
		} catch (Exception var2) {
			throw new RuntimeException(var2 + "转换Java Bean 为 HashMap错误");
		}
	}

	public static Map<String, Object> jsonToMap(String json, boolean collToString) throws JsonParseException {
		Map map = null;

		try {
			map = (Map) MAPPER.readValue(json, HashMap.class);
		} catch (IOException var5) {
			throw new RuntimeException(var5 + "转换Java Bean 为 HashMap错误");
		}

		if (collToString) {
			Iterator var3 = map.entrySet().iterator();

			while (true) {
				Entry entry;
				do {
					if (!var3.hasNext()) {
						return map;
					}

					entry = (Entry) var3.next();
				} while (!(entry.getValue() instanceof Collection) && !(entry.getValue() instanceof Map));

				entry.setValue(beanToJson(entry.getValue()));
			}
		} else {
			return map;
		}
	}

	public static String listToJson(List<Map<String, String>> list) throws JsonParseException {
		JsonGenerator jsonGenerator = null;
		StringWriter sw = new StringWriter();

		String var3;
		try {
			jsonGenerator = JSONFACTORY.createJsonGenerator(sw);
			(new ObjectMapper()).writeValue(jsonGenerator, list);
			jsonGenerator.flush();
			var3 = sw.toString();
		} catch (Exception var12) {
			throw new RuntimeException(var12 + "List 转换成json错误");
		} finally {
			if (jsonGenerator != null) {
				try {
					jsonGenerator.flush();
					jsonGenerator.close();
				} catch (Exception var11) {
					throw new RuntimeException(var11 + "List 转换成json错误");
				}
			}

		}

		return var3;
	}

	public static List<Map<String, String>> jsonToList(String json) throws JsonParseException {
		try {
			if (json != null && !"".equals(json.trim())) {
				JsonParser jsonParse = JSONFACTORY.createJsonParser(new StringReader(json));
				return (List) (new ObjectMapper()).readValue(jsonParse, ArrayList.class);
			} else {
				throw new RuntimeException("json 转List错误");
			}
		} catch (Exception var2) {
			throw new RuntimeException(var2 + "json 转List错误");
		}
	}

	public static List<Map<String, Object>> jsonToObjList(String json) throws JsonParseException {
		try {
			if (json != null && !"".equals(json.trim())) {
				JsonParser jsonParse = JSONFACTORY.createJsonParser(new StringReader(json));
				return (List) (new ObjectMapper()).readValue(jsonParse, ArrayList.class);
			} else {
				throw new RuntimeException("json 转List错误");
			}
		} catch (Exception var2) {
			throw new RuntimeException(var2 + "json 转List错误");
		}
	}

	static {
		MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		JSONFACTORY = new JsonFactory();
	}

}
