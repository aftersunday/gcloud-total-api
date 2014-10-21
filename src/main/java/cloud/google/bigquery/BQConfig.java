package cloud.google.bigquery;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collection;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;

public class BQConfig {

	public static final Collection<String> SCOPE = Arrays
			.asList(BigqueryScopes.BIGQUERY);
	public static final HttpTransport TRANSPORT = new NetHttpTransport();
	public static final JsonFactory JSON_FACTORY = new JacksonFactory();

	public static final String DATASET_ID = "test_again";
	public static final String PROJECT_ID = "build-again";

	public static final String SERVICE_ACCOUNT_ID = "120936437457-ivb1d5qf7tisnp84ieue6g87jpmvqh92@developer.gserviceaccount.com";
	public static final String PRIVATE_KEY = "75c220c9fef5a0955a6563976fc9bf705f20d0f1-privatekey.p12";

	private static Bigquery bigquery;

	public static Bigquery getBigquery() {
		return bigquery;
	}

	static {
		GoogleCredential credential;
		try {
			credential = new GoogleCredential.Builder()
					.setTransport(BQConfig.TRANSPORT)
					.setJsonFactory(BQConfig.JSON_FACTORY)
					.setServiceAccountId(BQConfig.SERVICE_ACCOUNT_ID)
					.setServiceAccountScopes(BQConfig.SCOPE)
					.setServiceAccountPrivateKeyFromP12File(
							new File(BQConfig.PRIVATE_KEY)).build();
			bigquery = new Bigquery.Builder(BQConfig.TRANSPORT,
					BQConfig.JSON_FACTORY, credential)
					.setApplicationName(BQConfig.PROJECT_ID)
					.setHttpRequestInitializer(credential).build();
		} catch (GeneralSecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}