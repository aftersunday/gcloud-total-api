/**
 	Copyright (C) Oct 6, 2014 xuanhung2401@gmail.com
 */
package cloud.google.taskqueue;

import java.io.IOException;
import java.security.GeneralSecurityException;

import cloud.google.config.APIConfig;
import cloud.google.oauth2.Authentication;
import cloud.google.util.ConnectionService;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * @author xuanhung2401
 * 
 */
public class RESTQueueHandle {

	private static final String QUEUE_NAME = "this-is-queue";
	@SuppressWarnings("unused")
	private static HttpTransport httpTransport;

	public static void main(String[] args) {
		try {
			httpTransport = GoogleNetHttpTransport.newTrustedTransport();
			// Task Queue Handle
			QueueItem item = new QueueItem();
			item.setId(System.currentTimeMillis() + "");
			item.setPayloadBase64(org.apache.commons.codec.binary.Base64
					.encodeBase64URLSafeString("youtube02.com".getBytes()));
			item.setQueueName(QUEUE_NAME);
			item.setTag("item-queue");
			item.setRetry_count(2);

			Gson gson = new GsonBuilder().create();
			String itemStr = gson.toJson(item);

			String result = ConnectionService
					.connect(
							"https://content.googleapis.com/taskqueue/v1beta2/projects/"
									+ APIConfig.getApplicationId()
									+ "/taskqueues/" + QUEUE_NAME + "/tasks")
					.body(itemStr)
					.accessToken(
							Authentication.getCredential().getAccessToken())
					.post();
			System.out.println(result);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (GeneralSecurityException e) {
			e.printStackTrace();
		}
	}

}
