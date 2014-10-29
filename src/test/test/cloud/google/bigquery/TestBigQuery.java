/**
 	Copyright (C) Oct 17, 2014 xuanhung2401@gmail.com
 */
package test.cloud.google.bigquery;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import test.cloud.google.entity.Foo;
import cloud.google.bigquery.BQCoreFunction;
import cloud.google.bigquery.BQInsert;
import cloud.google.util.Utility;

import com.google.api.services.bigquery.model.DatasetList.Datasets;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableList.Tables;
import com.google.api.services.bigquery.model.TableRow;

/**
 * @author xuanhung2401
 * 
 */
public class TestBigQuery {
	public static void main(String[] args) {
		List<Foo> list = testQuery2(Foo.class);
		System.out.println("--------------");
		for (Foo foo : list) {
			System.out.println("Name : " + foo.getName());
			System.out.println("Desc : " + foo.getDescription());
			System.out.println("Id : " + foo.getId());
			System.out.print("Interest : ");

			for (String i : foo.getInterest()) {
				System.out.print(i + ",");
			}
			System.out.println("");
			System.out.println("Age : " + foo.getAge());
			System.out.println("Gender : " + foo.isGender());
			System.out.println("Dob : " + foo.getDob());
		}
		// testInsert();
	}

	public static void testQuery() {
		try {
			JobReference jobReference = BQCoreFunction
					.createJob("SELECT id,name,age,dob FROM [test_again.Foo] where name = 'Foo 02' ORDER BY dob DESC limit 5");
			BQCoreFunction.checkQueryResults(jobReference.getJobId());
			GetQueryResultsResponse getQueryResultsResponse = BQCoreFunction
					.getQueryResults(jobReference.getJobId());

			BQCoreFunction.displayQueryResult(getQueryResultsResponse);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static <T> List<T> testQuery2(Class<T> clazz) {
		try {
			List<T> listResult = new ArrayList<T>();
			JobReference jobReference = BQCoreFunction
					.createJob("SELECT * FROM [test_again.Foo] where name = 'this_is_test_02'");
			BQCoreFunction.checkQueryResults(jobReference.getJobId());
			GetQueryResultsResponse getQueryResultsResponse = BQCoreFunction
					.getQueryResults(jobReference.getJobId());

			List<TableFieldSchema> listTableFieldSchema = getQueryResultsResponse
					.getSchema().getFields();

			List<TableRow> tableRows = getQueryResultsResponse.getRows();
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
					} else if (Utility.isBooleanField(fieldSchema.getType())) {
						try {
							objField.set(obj, Boolean
									.parseBoolean((String) tableCell.getV()));
						} catch (Exception e) {
							e.printStackTrace();
						}
					} else if (Utility.isFloatField(fieldSchema.getType())) {
						try {
							objField.set(obj,
									Float.parseFloat((String) tableCell.getV()));
						} catch (Exception e) {
							e.printStackTrace();
						}
					} else if (Utility.isDateTimeField(fieldSchema.getType())) {
						try {
							Calendar cal = Calendar.getInstance();
							double d = Double.parseDouble((String) tableCell
									.getV());
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

			return listResult;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

	}

	public static void testGetListDataset() {
		try {
			List<Datasets> list = BQCoreFunction.getListDatasets();
			for (Datasets datasets : list) {
				System.out.println(datasets.getDatasetReference()
						.getDatasetId());
			}
		} catch (Exception e) {
			// TODO: handle exception
		}

	}

	public static void testGetListTable() {
		try {
			List<Tables> list = BQCoreFunction
					.getListTable("build_again_domain");
			for (Tables datasets : list) {
				System.out.println(datasets.getTableReference().getTableId());
			}
		} catch (Exception e) {
			// TODO: handle exception
		}

	}

	public static void testInsert() {
		try {
			Foo f = new Foo();
			f.setName("this_is_test_03");
			f.setDescription("This is Test");

			System.out.println(BQInsert.insert(f));
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
}
