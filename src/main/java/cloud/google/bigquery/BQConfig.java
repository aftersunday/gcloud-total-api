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

	private static final Collection<String> SCOPE = Arrays
			.asList(BigqueryScopes.BIGQUERY);
	private static final HttpTransport TRANSPORT = new NetHttpTransport();
	private static final JsonFactory JSON_FACTORY = new JacksonFactory();
	private static String DATASET_ID = "test_again";
	private static String PROJECT_ID = "build-again";

	private static String SERVICE_ACCOUNT_ID = "120936437457-ivb1d5qf7tisnp84ieue6g87jpmvqh92@developer.gserviceaccount.com";
	private static String PRIVATE_KEY = "75c220c9fef5a0955a6563976fc9bf705f20d0f1-privatekey.p12";

	private static Bigquery bigquery;
	private static GoogleCredential credential;

	public Bigquery getBigquery() {
		return bigquery;
	}

	public String getDATASET_ID() {
		return DATASET_ID;
	}

	public String getPROJECT_ID() {
		return PROJECT_ID;
	}

	public String getSERVICE_ACCOUNT_ID() {
		return SERVICE_ACCOUNT_ID;
	}

	public String getPRIVATE_KEY() {
		return PRIVATE_KEY;
	}

	public void setDATASET_ID(String dATASET_ID) {
		DATASET_ID = dATASET_ID;
	}

	public void setPROJECT_ID(String pROJECT_ID) {
		PROJECT_ID = pROJECT_ID;
	}

	public void setSERVICE_ACCOUNT_ID(String sERVICE_ACCOUNT_ID) {
		SERVICE_ACCOUNT_ID = sERVICE_ACCOUNT_ID;
	}

	public void setPRIVATE_KEY(String pRIVATE_KEY) {
		PRIVATE_KEY = pRIVATE_KEY;
	}

	static {
		System.out.println("1st time credential...");
		try {
			credential = new GoogleCredential.Builder()
					.setTransport(BQConfig.TRANSPORT)
					.setJsonFactory(BQConfig.JSON_FACTORY)
					.setServiceAccountId(SERVICE_ACCOUNT_ID)
					.setServiceAccountScopes(BQConfig.SCOPE)
					.setServiceAccountPrivateKeyFromP12File(
							new File(PRIVATE_KEY)).build();
			bigquery = new Bigquery.Builder(BQConfig.TRANSPORT,
					BQConfig.JSON_FACTORY, credential)
					.setApplicationName(PROJECT_ID)
					.setHttpRequestInitializer(credential).build();
		} catch (GeneralSecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public BQConfig(String projectId, String datasetId,
			String serviceAccountId, String p12KeyLocation) {
		if (!projectId.equals(PROJECT_ID) || !datasetId.equals(DATASET_ID)
				|| !serviceAccountId.equals(SERVICE_ACCOUNT_ID)
				|| !p12KeyLocation.equals(PRIVATE_KEY) || credential == null) {
			PROJECT_ID = projectId;
			DATASET_ID = datasetId;
			SERVICE_ACCOUNT_ID = serviceAccountId;
			PRIVATE_KEY = p12KeyLocation;
			System.out.println("Init credential...");
			try {
				credential = new GoogleCredential.Builder()
						.setTransport(BQConfig.TRANSPORT)
						.setJsonFactory(BQConfig.JSON_FACTORY)
						.setServiceAccountId(SERVICE_ACCOUNT_ID)
						.setServiceAccountScopes(BQConfig.SCOPE)
						.setServiceAccountPrivateKeyFromP12File(
								new File(PRIVATE_KEY)).build();
				bigquery = new Bigquery.Builder(BQConfig.TRANSPORT,
						BQConfig.JSON_FACTORY, credential)
						.setApplicationName(PROJECT_ID)
						.setHttpRequestInitializer(credential).build();
			} catch (GeneralSecurityException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("Already credential !");
		}
	}
}