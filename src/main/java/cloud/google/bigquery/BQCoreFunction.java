package cloud.google.bigquery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cloud.google.util.Utility;

import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.DatasetList.Datasets;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableList.Tables;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class BQCoreFunction {

	/**
	 * Creates a Query Job for a particular query on a dataset
	 * 
	 * @param querySql
	 *            the actual query string
	 * @return a reference to the inserted query job
	 * @throws IOException
	 */
	public static JobReference createJob(String querySql) throws IOException {
		Job job = new Job();
		// Job need jobconfiguration

		JobConfiguration config = new JobConfiguration();

		// Jobconfiguration need JobConfigurationQuery
		JobConfigurationQuery queryConfig = new JobConfigurationQuery();
		queryConfig.setQuery(querySql);		

		config.setQuery(queryConfig);

		job.setConfiguration(config);

		// Insert job
		Insert insert = BQConfig.getBigquery().jobs()
				.insert(BQConfig.PROJECT_ID, job);
		insert.setProjectId(BQConfig.PROJECT_ID);

		// After execute get JobReference
		JobReference jobReference = insert.execute().getJobReference();
		return jobReference;
	}

	/**
	 * Polls the status of a BigQuery job, returns Job reference if "Done"
	 * 
	 * @param jobId
	 *            a reference to an inserted query Job
	 * @return a reference to the completed Job
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static Job checkQueryResults(String jobId) throws IOException,
			InterruptedException {
		// Variables to keep track of total query time
		long startTime = System.currentTimeMillis();
		long elapsedTime;
		while (true) {
			Job pollJob = BQConfig.getBigquery().jobs()
					.get(BQConfig.PROJECT_ID, jobId).execute();
			elapsedTime = System.currentTimeMillis() - startTime;
			System.out.format("Job status (%dms) %s: %s\n", elapsedTime, jobId,
					pollJob.getStatus().getState());
			if (pollJob.getStatus().getState().equals("DONE")) {
				return pollJob;
			}
			// Pause execution for one second before polling job status again,
			// to
			// reduce unnecessary calls to the BigQUery API and lower overall
			// application bandwidth.
			Thread.sleep(1000);
		}
	}

	/**
	 * Makes an API call to the BigQuery API
	 * 
	 * @param completedJob
	 *            to the completed Job
	 * @throws IOException
	 */
	public static GetQueryResultsResponse getQueryResults(String jobId)
			throws IOException {
		return BQConfig.getBigquery().jobs()
				.getQueryResults(BQConfig.PROJECT_ID, jobId).execute();
	}

	/**
	 * Just display query result
	 * */
	public static void displayQueryResult(
			GetQueryResultsResponse getQueryResultsResponse) {
		List<TableRow> rows = getQueryResultsResponse.getRows();
		System.out.print("\nQuery Results:\n------------\n");
		for (TableRow row : rows) {
			for (TableCell field : row.getF()) {
				System.out.printf("%-50s", field.getV());
			}
			System.out.println();
		}
	}

	/**
	 * Display all BigQuery table associated with dataset in project
	 * 
	 * @throws IOException
	 */
	public static List<Tables> getListTable(String datasetId)
			throws IOException {
		TableList tableList = BQConfig.getBigquery().tables()
				.list(BQConfig.PROJECT_ID, datasetId).execute();
		return tableList.getTables();
	}

	/**
	 * Display all BigQuery datasets associated with project
	 * 
	 * @throws IOException
	 */
	public static List<Datasets> getListDatasets() throws IOException {
		DatasetList datasetList = BQConfig.getBigquery().datasets()
				.list(BQConfig.PROJECT_ID).execute();
		return datasetList.getDatasets();
	}

	/**
	 * Create dataset in BigQuery project
	 * 
	 * @throws IOException
	 * */
	public static void createDataset() throws IOException {
		Dataset dataset = new Dataset();
		DatasetReference datasetRef = new DatasetReference();
		datasetRef.setProjectId(BQConfig.PROJECT_ID);
		datasetRef.setDatasetId(BQConfig.DATASET_ID);
		dataset.setDatasetReference(datasetRef);
		BQConfig.getBigquery().datasets().insert(BQConfig.PROJECT_ID, dataset)
				.execute();
	}

	/**
	 * Create table in a dataset based on Object type
	 * 
	 * @throws IOException
	 * */
	public static <T> void createTable(T obj, String tableName)
			throws IOException {
		// Create table schema
		TableSchema schema = new TableSchema();
		List<TableFieldSchema> tableFieldSchema = new ArrayList<TableFieldSchema>();
		TableFieldSchema schemaEntry;
		for (java.lang.reflect.Field f : obj.getClass().getDeclaredFields()) {
			schemaEntry = new TableFieldSchema();
			String fType = f.getType().getName();
			String fName = f.getName();
			if (Utility.isStringField(fType)) {
				schemaEntry.setName(fName);
				schemaEntry.setType("STRING");
				tableFieldSchema.add(schemaEntry);
			} else if (Utility.isIntegerField(fType)) {
				schemaEntry.setName(fName);
				schemaEntry.setType("INTEGER");
				tableFieldSchema.add(schemaEntry);
			} else if (Utility.isDoubleField(fType)) {
				schemaEntry.setName(fName);
				schemaEntry.setType("FLOAT");
				tableFieldSchema.add(schemaEntry);
			} else if (Utility.isBooleanField(fType)) {
				schemaEntry.setName(fName);
				schemaEntry.setType("BOOLEAN");
				tableFieldSchema.add(schemaEntry);
			} else if (Utility.isDateTimeField(fType)) {
				schemaEntry.setName(fName);
				schemaEntry.setType("TIMESTAMP");
				tableFieldSchema.add(schemaEntry);
			}
		}
		schema.setFields(tableFieldSchema);

		// Create table
		Table table = new Table();
		table.setSchema(schema);

		// Create table reference
		TableReference tableRef = new TableReference();
		tableRef.setDatasetId(BQConfig.PROJECT_ID);
		tableRef.setTableId(tableName);
		table.setTableReference(tableRef);

		BQConfig.getBigquery().tables()
				.insert(BQConfig.PROJECT_ID, BQConfig.DATASET_ID, table)
				.execute();
	}

}