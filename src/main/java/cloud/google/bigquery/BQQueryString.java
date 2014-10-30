package cloud.google.bigquery;

import java.util.List;
import java.util.logging.Logger;

import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.JobReference;

public class BQQueryString<T> {

	private static final Logger log = Logger.getLogger(BQQueryString.class
			.getName());

	private String strQuery;
	private Class<T> clazz;

	public BQQueryString(Class<T> clazz) {
		this.clazz = clazz;
	}

	public BQQueryString<T> queryString(String strQuery) {
		this.strQuery = strQuery;
		return this;
	}

	public List<T> list() {
		try {
			JobReference jobReference = BQCoreFunction.createJob(this.strQuery);
			BQCoreFunction.checkQueryResults(jobReference.getJobId());
			GetQueryResultsResponse getQueryResultsResponse = BQCoreFunction
					.getQueryResults(jobReference.getJobId());
			return BQCoreFunction.convertQueryResultToObject(this.clazz,
					getQueryResultsResponse);
		} catch (Exception e) {
			log.warning(e.getMessage());
			return null;
		}
	}

}