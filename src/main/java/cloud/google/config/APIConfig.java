/**
 	Copyright (C) Oct 6, 2014 xuanhung2401@gmail.com
 */
package cloud.google.config;

import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;

/**
 * @author xuanhung2401
 * 
 */
public class APIConfig {
	private static final String SERVICE_ACCOUNT_EMAIL = "303659554328-1isbui3nad3rr5v0hau11abr7i1l39u2@developer.gserviceaccount.com";
	private static final String KEY_LOCATION = "testblogerapi3-d7ea2f05e9a4.p12";
	private static final JsonFactory JSON_FACTORY = JacksonFactory
			.getDefaultInstance();
	private static final String APPLICATION_ID = "testblogerapi3";

	public static String getServiceAccountEmail() {
		return SERVICE_ACCOUNT_EMAIL;
	}

	public static String getKeyLocation() {
		return KEY_LOCATION;
	}

	public static JsonFactory getJsonFactory() {
		return JSON_FACTORY;
	}

	public static String getApplicationId() {
		return APPLICATION_ID;
	}

}
