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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

public class ConnectionService {

	private static final Logger log = Logger.getLogger(ConnectionService.class
			.getName());

	private String url = "";
	private Map<String, String> headers = new HashMap<String, String>();
	private String method = "GET";
	private String requestBody = "";
	private int acceptResponseCode = 200;

	public ConnectionService() {
		this.header("Content-Type", "application/json");
	}

	public ConnectionService url(String url) {
		this.setUrl(url);
		return this;
	}

	public String url() {
		return this.url;
	}

	public ConnectionService acceptCode(int value) {
		this.acceptResponseCode = value;
		return this;
	}

	public ConnectionService header(String name, String value) {
		this.headers.put(name, value);
		return this;
	}

	public ConnectionService accessToken(String value) {
		this.headers.put("Authorization", "Bearer " + value);
		return this;
	}

	public Map<String, String> headers() {
		return this.headers;
	}

	public String method() {
		return this.method;
	}

	public ConnectionService method(String method) {
		this.method = method;
		return this;
	}

	public ConnectionService body(String body) {
		this.requestBody = body;
		return this;
	}

	public String body() {
		return this.requestBody;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String get() {
		this.method("GET");
		return this.execute();
	}

	public String post() {
		this.method("POST");
		return this.execute();
	}

	/**
	 * Open connection to this.url, use gzip to send data. Check response code
	 * and return response string (using ByteArrayOutputStream to read)
	 * */
	public String execute() {
		HttpURLConnection connection = null;
		try {
			URL url = new URL(this.url);
			connection = (HttpURLConnection) url.openConnection();
			connection.setDoOutput(true);
			connection.setRequestMethod(this.method);
			for (Entry<String, String> entry : headers.entrySet()) {
				connection.addRequestProperty(entry.getKey(), entry.getValue());
			}

			/**
			 * write request body, add parameter
			 * */
			OutputStreamWriter wr = new OutputStreamWriter(
					connection.getOutputStream());
			wr.write(requestBody);
			wr.flush();

			connection.connect();

			int statusCode = connection.getResponseCode();
			InputStream stream = null;
			if (statusCode != acceptResponseCode) {
				stream = connection.getErrorStream();
			} else {
				stream = connection.getInputStream();
			}

			ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			int length;
			byte[] b = new byte[2048];
			while ((length = stream.read(b)) != -1) {
				buffer.write(b, 0, length);
			}
			buffer.flush();
			if (null != connection) {
				connection.disconnect();
			}
			if (statusCode != acceptResponseCode) {
				log.severe("Request return code : " + statusCode + "\n"
						+ buffer.toString("UTF-8"));
				return null;
			} else {
				return buffer.toString("UTF-8");
			}
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Return connectionservice object after set url = user parameter
	 * */
	public static ConnectionService connect(String url) {
		ConnectionService instance = new ConnectionService();
		instance.setUrl(url);
		return instance;
	}

}
