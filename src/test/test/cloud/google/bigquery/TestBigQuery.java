/**
 	Copyright (C) Oct 17, 2014 xuanhung2401@gmail.com
 */
package test.cloud.google.bigquery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import test.cloud.google.entity.Foo;
import cloud.google.bigquery.BQConfig;
import cloud.google.bigquery.BQCoreFunction;
import cloud.google.bigquery.BQInsert;
import cloud.google.bigquery.BQQuery;

import com.google.api.services.bigquery.model.DatasetList.Datasets;
import com.google.api.services.bigquery.model.TableList.Tables;

/**
 * @author xuanhung2401
 * 
 */
public class TestBigQuery {

	static String project = "build-again";
	static String dataset = "test_again";
	static String account = "120936437457-ivb1d5qf7tisnp84ieue6g87jpmvqh92@developer.gserviceaccount.com";
	static String key = "75c220c9fef5a0955a6563976fc9bf705f20d0f1-privatekey.p12";

	public static void main(String[] args) throws IOException {
		BQConfig config = new BQConfig(project, dataset, account, key);
		BQQuery<Foo> bq = new BQQuery<Foo>(config);
		System.out.println(bq.sqlQuery(Foo.class)
				.queryString("SELECT COUNT(*) from [test_again.Foo]").result()
				.getRows().get(0).getF().get(0).getV());
		config = new BQConfig(project, "domain", account, key);
	}

	public static void testQueryString() {

	}

	public static void testGetListDataset() {
		try {
			BQConfig config = new BQConfig(project, dataset, account, key);
			BQCoreFunction function = new BQCoreFunction(config);
			List<Datasets> list = function.getListDatasets();
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
			BQConfig config = new BQConfig(project, dataset, account, key);
			BQCoreFunction function = new BQCoreFunction(config);
			List<Tables> list = function.getListTable("build_again_domain");
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
			BQConfig config = new BQConfig(project, dataset, account, key);
			BQInsert insertor = new BQInsert(config);
			System.out.println(insertor.insert(list));
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
}
