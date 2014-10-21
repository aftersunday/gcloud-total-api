/**
 	Copyright (C) Oct 17, 2014 xuanhung2401@gmail.com
 */
package test.cloud.google.bigquery;

import java.util.ArrayList;
import java.util.List;

import test.cloud.google.entity.Foo;
import cloud.google.bigquery.BQCoreFunction;
import cloud.google.bigquery.BQInsert;

import com.google.api.services.bigquery.model.DatasetList.Datasets;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableList.Tables;

/**
 * @author xuanhung2401
 * 
 */
public class TestBigQuery {
	public static void main(String[] args) {
		testQuery();
	}

	public static void testQuery() {
		try {
			JobReference jobReference = BQCoreFunction
					.createJob("SELECT * FROM test_again.Foo");
			BQCoreFunction.checkQueryResults(jobReference.getJobId());
			GetQueryResultsResponse getQueryResultsResponse = BQCoreFunction
					.getQueryResults(jobReference.getJobId());

			BQCoreFunction.displayQueryResult(getQueryResultsResponse);
		} catch (Exception e) {
			// TODO: handle exception
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
			f.setName("No Foo 03");
			f.setDescription("No Foo 03");
			Foo f1 = new Foo();
			f1.setName("No Foo 04");
			f1.setDescription("No Foo 04");
			List<Foo> list = new ArrayList<Foo>();
			list.add(f);
			list.add(f1);
			System.out.println(BQInsert.insert(list));
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
}
