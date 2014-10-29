package cloud.google.bigquery;

import java.util.List;
import java.util.logging.Logger;

import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;

public class BQQuery<T> {

	private static final Logger log = Logger.getLogger(BQQuery.class.getName());

	public static void query(String strQuery) {
		try {
			JobReference jobReference = BQCoreFunction.createJob(strQuery);
			BQCoreFunction.checkQueryResults(jobReference.getJobId());
			GetQueryResultsResponse getQueryResultsResponse = BQCoreFunction
					.getQueryResults(jobReference.getJobId());
			List<TableRow> rows = getQueryResultsResponse.getRows();
			for (TableRow row : rows) {
				for (TableCell field : row.getF()) {					
					System.out.printf("%-50s", field.getV());
				}
				System.out.println();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		log.warning(strQuery);
	}

	public static void convertQueryResultToObject() {

	}
}