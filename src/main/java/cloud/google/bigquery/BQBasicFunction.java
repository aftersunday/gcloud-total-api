package cloud.google.bigquery;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.security.GeneralSecurityException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import cloud.google.annotation.Annotation.BigQuery_Id;
import cloud.google.annotation.Annotation.BigQuery_Remove;
import cloud.google.util.Utility;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest.Rows;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class BQBasicFunction {

	private static final Collection<String> SCOPE = new ArrayList<String>();
	private static final HttpTransport TRANSPORT = new NetHttpTransport();
	private static final JsonFactory JSON_FACTORY = new JacksonFactory();
	private static final String SERVICE_ACCOUNT_ID = "120936437457-ivb1d5qf7tisnp84ieue6g87jpmvqh92@developer.gserviceaccount.com";
	private static final String PRIVATE_KEY = "75c220c9fef5a0955a6563976fc9bf705f20d0f1-privatekey.p12";
	private static final String DATASET_ID = "test_again";
	private static final String PROJECT_ID = "build-again";

	private static Bigquery bigquery;

	static {
		SCOPE.add("https://www.googleapis.com/auth/bigquery");
		GoogleCredential credential;
		try {
			credential = new GoogleCredential.Builder()
					.setTransport(TRANSPORT)
					.setJsonFactory(JSON_FACTORY)
					.setServiceAccountId(SERVICE_ACCOUNT_ID)
					.setServiceAccountScopes(
							Collections.singleton(BigqueryScopes.BIGQUERY))
					.setServiceAccountPrivateKeyFromP12File(
							new File(PRIVATE_KEY)).build();
			bigquery = new Bigquery.Builder(TRANSPORT, JSON_FACTORY, credential)
					.setApplicationName(PROJECT_ID)
					.setHttpRequestInitializer(credential).build();

		} catch (GeneralSecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Rows convertObjectToTableRows(Object obj) {
		TableDataInsertAllRequest.Rows rows = new TableDataInsertAllRequest.Rows();
		TableRow row = new TableRow();
		String objectId = "";
		for (Field f : obj.getClass().getDeclaredFields()) {
			f.setAccessible(true);
			String fName = f.getName();
			String fType = f.getType().getName();
			Object fValue = null;

			if (f.isAnnotationPresent(BigQuery_Id.class)) {
				try {
					if (f.get(obj) != null) {
						objectId = (String) f.get(obj);
					}
				} catch (Exception e) {
					objectId = "";
				}
			}

			if (f.isAnnotationPresent(BigQuery_Remove.class)) {
				continue;
			}

			if (Utility.isStringField(fType)) {
				try {
					if (f.get(obj) != null) {
						fValue = (String) f.get(obj);
					}
				} catch (Exception e) {
					fValue = "";
				}
			} else if (Utility.isIntegerField(fType)) {
				try {
					if (f.get(obj) != null) {
						fValue = f.getInt(obj);
					}
				} catch (Exception e) {
					fValue = 0;
				}
			} else if (Utility.isDateTimeField(fType)) {
				SimpleDateFormat dateFormat = new SimpleDateFormat(
						"YYYY-MM-dd HH:mm:ss.SSS");
				try {
					if (f.get(obj) != null) {
						Date date = (Date) f.get(obj);
						fValue = dateFormat.format(date);
					}
				} catch (Exception e) {
					Date date = Calendar.getInstance().getTime();
					fValue = dateFormat.format(date);
				}
			} else if (Utility.isBooleanField(fType)) {
				try {
					if (f.get(obj) != null) {
						fValue = (Boolean) f.get(obj);
					}
				} catch (Exception e) {
					fValue = true;
				}
			} else if (Utility.isDoubleField(fType)
					|| Utility.isLongField(fType)) {
				try {
					if (f.get(obj) != null) {
						fValue = (Double) f.get(obj);
					}
				} catch (Exception e) {
					fValue = 0.0;
				}
			}
			row.set(fName, fValue);
		}
		rows.setInsertId(objectId);
		rows.setJson(row);
		return rows;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void insert(Object obj) {
		Rows rows = convertObjectToTableRows(obj);
		List rowList = new ArrayList();
		rowList.add(rows);

		TableDataInsertAllRequest content = new TableDataInsertAllRequest()
				.setRows(rowList);
		try {
			TableDataInsertAllResponse response = bigquery
					.tabledata()
					.insertAll(PROJECT_ID, DATASET_ID,
							obj.getClass().getSimpleName(), content).execute();
			System.out.println(response.toPrettyString());
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Insert bigquery fail - " + rows.getInsertId());
		}
	}

	public static void createDataset() throws IOException {
		Dataset dataset = new Dataset();
		DatasetReference datasetRef = new DatasetReference();
		datasetRef.setProjectId(PROJECT_ID);
		datasetRef.setDatasetId(DATASET_ID);
		dataset.setDatasetReference(datasetRef);
		try {
			bigquery.datasets().insert(PROJECT_ID, dataset).execute();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void createTable(Object obj, String tableName)
			throws IOException {
		TableSchema schema = new TableSchema();
		List<TableFieldSchema> tableFieldSchema = new ArrayList<TableFieldSchema>();
		TableFieldSchema schemaEntry = new TableFieldSchema();

		for (java.lang.reflect.Field f : obj.getClass().getDeclaredFields()) {
			String fName = f.getName();
			if (f.getType().getSimpleName().equals("String")) {
				schemaEntry = new TableFieldSchema();
				schemaEntry.setName(fName);
				schemaEntry.setType("STRING");
				tableFieldSchema.add(schemaEntry);
			} else if (f.getType().getSimpleName().equals("int")) {
				schemaEntry = new TableFieldSchema();
				schemaEntry.setName(fName);
				schemaEntry.setType("INTEGER");
				tableFieldSchema.add(schemaEntry);
			} else if (f.getType().getSimpleName().equals("Double")
					|| f.getType().getSimpleName().equals("double")) {
				schemaEntry = new TableFieldSchema();
				schemaEntry.setName(fName);
				schemaEntry.setType("FLOAT");
				tableFieldSchema.add(schemaEntry);
			} else if (f.getType().getSimpleName().equals("boolean")) {
				schemaEntry = new TableFieldSchema();
				schemaEntry.setName(fName);
				schemaEntry.setType("BOOLEAN");
				tableFieldSchema.add(schemaEntry);
			} else if (f.getType().getSimpleName().equals("Date")) {
				schemaEntry = new TableFieldSchema();
				schemaEntry.setName(fName);
				schemaEntry.setType("TIMESTAMP");
				tableFieldSchema.add(schemaEntry);
			}
		}
		schema.setFields(tableFieldSchema);

		Table table = new Table();
		table.setSchema(schema);
		TableReference tableRef = new TableReference();
		tableRef.setDatasetId(DATASET_ID);
		tableRef.setProjectId(PROJECT_ID);
		tableRef.setTableId(tableName);
		table.setTableReference(tableRef);
		try {
			bigquery.tables().insert(PROJECT_ID, DATASET_ID, table).execute();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}