package cloud.google.bigquery;

import java.util.List;
import java.util.logging.Logger;

import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.JobReference;

public class BQSQLQuery<T> extends BQQuery<T> {

	/**
	 * @param config
	 */
	public BQSQLQuery(BQConfig config) {
		super(config);
	}

	private static final Logger log = Logger.getLogger(BQSQLQuery.class
			.getName());

	private String strQuery;
	private Class<T> clazz;

	public BQSQLQuery<T> queryString(String strQuery) {
		this.strQuery = strQuery;
		return this;
	}

	public List<T> list() {
		try {
			BQCoreFunction coreFunction = new BQCoreFunction(this.config);
			JobReference jobReference = coreFunction.createJob(this.strQuery);
			coreFunction.checkQueryResults(jobReference.getJobId());
			GetQueryResultsResponse getQueryResultsResponse = coreFunction
					.getQueryResults(jobReference.getJobId());
			return BQCoreFunction.convertQueryResultToObject(this.clazz,
					getQueryResultsResponse);
		} catch (Exception e) {
			log.warning(e.getMessage());
			return null;
		}
	}

	public GetQueryResultsResponse result() {
		try {
			BQCoreFunction coreFunction = new BQCoreFunction(this.config);
			JobReference jobReference = coreFunction.createJob(this.strQuery);
			coreFunction.checkQueryResults(jobReference.getJobId());
			return coreFunction.getQueryResults(jobReference.getJobId());
		} catch (Exception e) {
			log.warning(e.getMessage());
			return null;
		}
	}

}