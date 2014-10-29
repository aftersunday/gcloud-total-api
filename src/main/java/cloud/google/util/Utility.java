/**
 * Copyright (C) 2014 xuanhung2401.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cloud.google.util;

/**
 * @author xuanhung2401
 * 
 */

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import com.google.gson.Gson;

public class Utility {

	public static boolean isIntegerField(String typeName) {
		if (typeName.equals("INTEGER") || typeName.equals("int")
				|| typeName.equals("class java.lang.Integer")
				|| typeName.equals("java.lang.Integer")) {
			return true;
		}
		return false;
	}

	public static boolean isStringField(String typeName) {
		if (typeName.equals("STRING")
				|| typeName.equals("class java.lang.String")
				|| typeName.equals("java.lang.String")) {
			return true;
		}
		return false;
	}

	public static boolean isBooleanField(String typeName) {
		if (typeName.equals("BOOLEAN")
				|| typeName.equals("class java.lang.Boolean")
				|| typeName.equals("boolean")
				|| typeName.equals("java.lang.Boolean")) {
			return true;
		}
		return false;
	}

	public static boolean isDateTimeField(String typeName) {
		if (typeName.equals("TIMESTAMP")
				|| typeName.equals("class java.util.Date")
				|| typeName.equals("java.util.Date")) {
			return true;
		}
		return false;
	}

	public static boolean isDoubleField(String typeName) {
		if (typeName.equals("DOUBLE")
				|| typeName.equals("class java.lang.Double")
				|| typeName.equals("double")
				|| typeName.equals("java.lang.Double")) {
			return true;
		}
		return false;
	}

	public static boolean isFloatField(String typeName) {
		if (typeName.equals("FLOAT")) {
			return true;
		}
		return false;
	}

	public static boolean isLongField(String typeName) {
		if (typeName.equals("LONG") || typeName.equals("class java.lang.Long")
				|| typeName.equals("java.lang.Long") || typeName.equals("long")) {
			return true;
		}
		return false;
	}

	public static boolean isListField(String typeName) {
		if (typeName.contains("java.util.List")
				|| typeName.contains("java.util.ArrayList")) {
			return true;
		}
		return false;
	}

	public static String generateGoogleDataType(String originTypeName) {
		if (isListField(originTypeName)) {
			return "listValue";
		} else if (isIntegerField(originTypeName)) {
			return "integerValue";
		} else if (isDoubleField(originTypeName)) {
			return "doubleValue";
		} else if (isBooleanField(originTypeName)) {
			return "booleanValue";
		} else if (isDateTimeField(originTypeName)) {
			return "dateTimeValue";
		} else if (isStringField(originTypeName)) {
			return "stringValue";
		}
		return "stringValue";
	}

	public static <T> T fromJsonToObject(Class<T> clazz, String jsonStr) {
		try {
			return new Gson().fromJson(jsonStr, clazz);
		} catch (Exception e) {
			return null;
		}
	}

	public static <T> List<List<T>> splitList(List<T> parent, int subSize) {
		List<List<T>> result = new ArrayList<List<T>>();
		List<T> sub = new ArrayList<T>();
		for (int i = 0; i < parent.size(); i++) {
			sub.add(parent.get(i));
			if (sub.size() == subSize || (parent.size() - 1) == i) {
				result.add(sub);
				sub = new ArrayList<T>();
			}
		}
		return result;
	}

	public static void main(String[] args) {
		String ts = "1.414399417544E9";
		double d1 = Double.parseDouble(ts);
		System.out.println(d1);
		long l1 = (long) d1 * 1000;

		System.out.println(l1);
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(l1);
		System.out.println(cal.getTime());
	}
}
