/**
 	Copyright (C) Oct 6, 2014 xuanhung2401@gmail.com
 */
package cloud.google.oauth2;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.logging.Logger;

import cloud.google.config.APIConfig;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;

/**
 * @author xuanhung2401
 * 
 */
public class Authentication {

	private static final String SCOPE = "https://www.googleapis.com/auth/taskqueue";

	private static final Logger log = Logger.getLogger(Authentication.class
			.getName());

	private static HttpTransport httpTransport;

	private static GoogleCredential credential = null;

	static {
		try {
			httpTransport = GoogleNetHttpTransport.newTrustedTransport();
			if (credential == null) {
				credential = new GoogleCredential.Builder()
						.setTransport(httpTransport)
						.setJsonFactory(APIConfig.getJsonFactory())
						.setServiceAccountId(APIConfig.getServiceAccountEmail())
						.setServiceAccountScopes(Collections.singleton(SCOPE))
						.setServiceAccountPrivateKeyFromP12File(
								new File(APIConfig.getKeyLocation())).build();
			}
		} catch (GeneralSecurityException e) {
			log.severe(e.getMessage());
		} catch (IOException e) {
			log.severe(e.getMessage());
		}
	}

	public static GoogleCredential getCredential() {
		return credential;
	}

	public static HttpTransport getHttpTransport() {
		return httpTransport;
	}

}
