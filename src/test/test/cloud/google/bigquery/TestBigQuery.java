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
import cloud.google.bigquery.BQQuery;
import cloud.google.bigquery.FilterOperator;
import cloud.google.bigquery.OrderDirection;
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
		// List<Foo> list = testQuery2(Foo.class);
		// System.out.println("--------------");
		// for (Foo foo : list) {
		// System.out.println("Name : " + foo.getName());
		// System.out.println("Desc : " + foo.getDescription());
		// System.out.println("Id : " + foo.getId());
		// System.out.print("Interest : ");
		//
		// for (String i : foo.getInterest()) {
		// System.out.print(i + ",");
		// }
		// System.out.println("");
		// System.out.println("Age : " + foo.getAge());
		// System.out.println("Gender : " + foo.isGender());
		// System.out.println("Dob : " + foo.getDob());
		// }

		// testInsert();
		//
		// try {
		// BQCoreFunction.createTable(Foo.class);
		// } catch (Exception e) {
		// // TODO: handle exception
		// }
		BQQuery<Foo> bq = new BQQuery<Foo>();
		List<Foo> list = bq.query(Foo.class).projection("name")
				.projection("email")
				.filter("randomInt", 20, FilterOperator.GREATER_THAN_OR_EQUAL)
				.limit(20).list();
		for (Foo foo : list) {
			System.out.println(foo.getName() + " " + foo.getEmail());
		}
	}

	public static void testQueryString() {

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
			List<Foo> list = new ArrayList<Foo>();
			Foo f = new Foo();
			f.setName("Lan");
			f.setEmail("lan@gmail.com");
			f.setRandomInt(24);
			f.setRandomDouble(3.0);
			f.setGender(false);
			Calendar cal = Calendar.getInstance();
			cal.set(1990, 8, 24);
			f.setDob(cal.getTime());
			list.add(f);

			f = new Foo();
			f.setName("Trang");
			f.setEmail("trang@gmail.com");
			f.setRandomInt(22);
			f.setRandomDouble(4.0);
			f.setGender(true);
			cal = Calendar.getInstance();
			cal.set(1990, 12, 23);
			f.setDob(cal.getTime());
			list.add(f);

			f = new Foo();
			f.setName("Linh");
			f.setEmail("linh@gmail.com");
			f.setRandomInt(27);
			f.setRandomDouble(6.0);
			f.setGender(true);
			cal = Calendar.getInstance();
			cal.set(1987, 1, 24);
			f.setDob(cal.getTime());
			list.add(f);

			f = new Foo();
			f.setName("Ngoc");
			f.setEmail("ngoc@gmail.com");
			f.setRandomInt(27);
			f.setRandomDouble(3.0);
			f.setGender(true);
			cal = Calendar.getInstance();
			cal.set(1987, 2, 14);
			f.setDob(cal.getTime());
			list.add(f);

			f = new Foo();
			f.setName("Hoa");
			f.setEmail("hoa@gmail.com");
			f.setRandomInt(22);
			f.setRandomDouble(4.0);
			f.setGender(false);
			cal = Calendar.getInstance();
			cal.set(1995, 3, 14);
			f.setDob(cal.getTime());
			list.add(f);

			f = new Foo();
			f.setName("Mai");
			f.setEmail("mai@gmail.com");
			f.setRandomInt(22);
			f.setRandomDouble(4.0);
			f.setGender(false);
			cal = Calendar.getInstance();
			cal.set(1995, 3, 22);
			f.setDob(cal.getTime());
			list.add(f);
			System.out.println(list.size());

			System.out.println(BQInsert.insert(list));
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
}
