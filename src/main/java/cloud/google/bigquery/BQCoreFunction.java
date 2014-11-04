package cloud.google.bigquery;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
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

	private BQConfig config;

	/**
	 * @param config
	 */
	public BQCoreFunction(BQConfig config) {
		this.config = config;
	}

	/**
	 * Creates a Query Job for a particular query on a dataset
	 * 
	 * @param querySql
	 *            the actual query string
	 * @return a reference to the inserted query job
	 * @throws IOException
	 */
	public JobReference createJob(String querySql) throws IOException {
		Job job = new Job();
		// Job need jobconfiguration

		JobConfiguration config = new JobConfiguration();

		// Jobconfiguration need JobConfigurationQuery
		JobConfigurationQuery queryConfig = new JobConfigurationQuery();
		queryConfig.setQuery(querySql);

		config.setQuery(queryConfig);

		job.setConfiguration(config);

		// Insert job
		Insert insert = this.config.getBigquery().jobs()
				.insert(this.config.getPROJECT_ID(), job);
		insert.setProjectId(this.config.getPROJECT_ID());

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
	public Job checkQueryResults(String jobId) throws IOException,
			InterruptedException {
		// Variables to keep track of total query time
		long startTime = System.currentTimeMillis();
		long elapsedTime;
		while (true) {
			Job pollJob = this.config.getBigquery().jobs()
					.get(this.config.getPROJECT_ID(), jobId).execute();
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
	public GetQueryResultsResponse getQueryResults(String jobId)
			throws IOException {
		return this.config.getBigquery().jobs()
				.getQueryResults(this.config.getPROJECT_ID(), jobId).execute();
	}

	/**
	 * Just display query result
	 * */
	public void displayQueryResult(
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
	public List<Tables> getListTable(String datasetId) throws IOException {
		TableList tableList = this.config.getBigquery().tables()
				.list(this.config.getPROJECT_ID(), datasetId).execute();
		return tableList.getTables();
	}

	/**
	 * Display all BigQuery datasets associated with project
	 * 
	 * @throws IOException
	 */
	public List<Datasets> getListDatasets() throws IOException {
		DatasetList datasetList = this.config.getBigquery().datasets()
				.list(this.config.getPROJECT_ID()).execute();
		return datasetList.getDatasets();
	}

	/**
	 * Create dataset in BigQuery project
	 * 
	 * @throws IOException
	 * */
	public void createDataset() throws IOException {
		Dataset dataset = new Dataset();
		DatasetReference datasetRef = new DatasetReference();
		datasetRef.setProjectId(this.config.getPROJECT_ID());
		datasetRef.setDatasetId(this.config.getDATASET_ID());
		dataset.setDatasetReference(datasetRef);
		this.config.getBigquery().datasets()
				.insert(this.config.getPROJECT_ID(), dataset).execute();
	}

	/**
	 * Create table in a dataset based on Object type
	 * 
	 * @param <T>
	 * 
	 * @throws IOException
	 * */
	public <T> void createTable(Class<T> obj) throws IOException {
		// Create table schema
		TableSchema schema = new TableSchema();
		List<TableFieldSchema> tableFieldSchema = new ArrayList<TableFieldSchema>();
		TableFieldSchema schemaEntry;
		for (java.lang.reflect.Field f : obj.getDeclaredFields()) {
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
		tableRef.setDatasetId(this.config.getDATASET_ID());
		tableRef.setTableId(obj.getSimpleName());
		table.setTableReference(tableRef);

		this.config
				.getBigquery()
				.tables()
				.insert(this.config.getPROJECT_ID(),
						this.config.getDATASET_ID(), table).execute();
	}

	public static <T> List<T> convertQueryResultToObject(Class<T> clazz,
			GetQueryResultsResponse queryResultsResponse) {
		try {
			List<T> listResult = new ArrayList<T>();
			List<TableFieldSchema> listTableFieldSchema = queryResultsResponse
					.getSchema().getFields();

			List<TableRow> tableRows = queryResultsResponse.getRows();
			if (tableRows != null && tableRows.size() > 0) {
				for (TableRow tableRow : tableRows) {
					Object obj = Class.forName(clazz.getName()).newInstance();
					int count = 0;
					for (TableCell tableCell : tableRow.getF()) {

						TableFieldSchema fieldSchema = listTableFieldSchema
								.get(count);
						Field objField = obj.getClass().getDeclaredField(
								fieldSchema.getName());
						objField.setAccessible(true);
						if (Utility.isStringField(fieldSchema.getType())) {
							try {
								objField.set(obj, tableCell.getV().toString());
							} catch (Exception e) {
								e.printStackTrace();
							}
						} else if (Utility
								.isIntegerField(fieldSchema.getType())) {
							try {
								objField.set(obj, Integer
										.parseInt((String) tableCell.getV()));
							} catch (Exception e) {
								e.printStackTrace();
							}
						} else if (Utility
								.isBooleanField(fieldSchema.getType())) {
							try {
								objField.set(obj,
										Boolean.parseBoolean((String) tableCell
												.getV()));
							} catch (Exception e) {
								e.printStackTrace();
							}
						} else if (Utility.isDoubleField(fieldSchema.getType())) {
							try {
								objField.set(obj, Double
										.parseDouble((String) tableCell.getV()));
							} catch (Exception e) {
								System.out.println(e.getMessage());
							}
						} else if (Utility.isFloatField(fieldSchema.getType())) {
							try {
								if (Utility.isDoubleField(objField.getType()
										.getSimpleName())) {
									objField.set(obj, Double
											.parseDouble((String) tableCell
													.getV()));
								} else {
									objField.set(obj, Float
											.parseFloat((String) tableCell
													.getV()));
								}
							} catch (Exception e) {
								System.out.println(e.getMessage());
							}
						} else if (Utility.isDateTimeField(fieldSchema
								.getType())) {
							try {
								Calendar cal = Calendar.getInstance();
								double d = Double
										.parseDouble((String) tableCell.getV());
								long l = (long) d * 1000;
								cal.setTimeInMillis(l);
								objField.set(obj, cal.getTime());
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
						count++;
					}
					listResult.add(clazz.cast(obj));
				}
			}

			return listResult;
		} catch (Exception e) {
			e.printStackTrace();
			return new ArrayList<T>();
		}
	}
}