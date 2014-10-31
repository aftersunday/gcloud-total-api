package cloud.google.bigquery;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import cloud.google.util.Utility;

import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.JobReference;

public class BQObjectQuery<T> extends BQQuery<T> {

	/**
	 * @param config
	 */
	public BQObjectQuery(BQConfig config) {
		super(config);
	}

	private static final Logger log = Logger.getLogger(BQObjectQuery.class
			.getName());

	private int offset = -1;
	private int limit = -1;
	private List<String> projection;
	private List<String> filter;
	private List<String> order;
	private Class<T> clazz;

	public BQObjectQuery<T> offset(int offset) {
		this.offset = offset;
		return this;
	}

	public BQObjectQuery<T> limit(int limit) {
		this.limit = limit;
		return this;
	}

	public BQObjectQuery<T> projection(List<String> projection) {
		if (this.projection == null || this.projection.size() == 0) {
			this.projection = new ArrayList<String>();
		}
		for (String str : projection) {
			this.projection.add(str);
		}
		return this;
	}

	public BQObjectQuery<T> projection(String... projection) {
		if (this.projection == null || this.projection.size() == 0) {
			this.projection = new ArrayList<String>();
		}
		for (String str : projection) {
			this.projection.add(str);
		}
		return this;
	}

	public BQObjectQuery<T> filter(String field, Object value,
			BQFilterOperator operator) {
		if (this.filter == null || this.filter.size() == 0) {
			this.filter = new ArrayList<String>();
		}
		StringBuilder builder = new StringBuilder();
		builder.append(field);
		builder.append(" ");
		builder.append(operator.getSymbol());
		builder.append(" ");
		String dataType = value.getClass().toString();
		if (Utility.isDoubleField(dataType) || Utility.isBooleanField(dataType)
				|| Utility.isIntegerField(dataType)) {
			builder.append(value);
		} else if (Utility.isDateTimeField(dataType)
				|| Utility.isStringField(dataType)) {
			builder.append("'");
			builder.append(value);
			builder.append("'");
		}
		this.filter.add(builder.toString());
		return this;
	}

	public BQObjectQuery<T> order(String field, BQOrderDirection direction) {
		if (this.order == null || this.order.size() == 0) {
			this.order = new ArrayList<String>();
		}
		StringBuilder builder = new StringBuilder();
		builder.append(field);
		builder.append(" ");
		builder.append(direction.name());
		this.order.add(builder.toString());
		return this;
	}

	public List<T> list() {
		StringBuilder strQuery = new StringBuilder();
		strQuery.append("SELECT");
		strQuery.append(" ");
		if (this.projection != null && this.projection.size() > 0) {
			for (int i = 0; i < this.projection.size(); i++) {
				strQuery.append(this.projection.get(i));
				if (i + 1 < this.projection.size()) {
					strQuery.append(",");
				}
			}
		} else {
			strQuery.append("*");
		}
		strQuery.append(" ");
		if (this.clazz != null) {
			strQuery.append("FROM");
			strQuery.append(" ");
			strQuery.append(this.config.getDATASET_ID());
			strQuery.append(".");
			strQuery.append(this.clazz.getSimpleName());
			strQuery.append(" ");
		}
		if (filter != null && filter.size() > 0) {
			strQuery.append("WHERE");
			strQuery.append(" ");
			for (int i = 0; i < this.filter.size(); i++) {
				strQuery.append(this.filter.get(i));
				if (i + 1 < this.filter.size()) {
					strQuery.append(" ");
					strQuery.append("AND");
					strQuery.append(" ");
				}
			}
			strQuery.append(" ");
		}
		if (this.order != null && this.order.size() > 0) {
			strQuery.append("ORDER BY");
			strQuery.append(" ");
			for (int i = 0; i < this.order.size(); i++) {
				strQuery.append(this.order.get(i));
				if (i + 1 < this.order.size()) {
					strQuery.append(",");
					strQuery.append(" ");
				}
			}
		}
		if (this.offset != -1) {
			strQuery.append("OFFSET");
			strQuery.append(" ");
			strQuery.append(this.offset);
			strQuery.append(" ");
		}
		if (this.limit != -1) {
			strQuery.append("LIMIT");
			strQuery.append(" ");
			strQuery.append(this.limit);
		}
		try {
			BQCoreFunction coreFunction = new BQCoreFunction(this.config);
			JobReference jobReference = coreFunction.createJob(strQuery
					.toString());
			coreFunction.checkQueryResults(jobReference.getJobId());
			GetQueryResultsResponse getQueryResultsResponse = coreFunction
					.getQueryResults(jobReference.getJobId());
			List<T> list = new ArrayList<T>();
			list = BQCoreFunction.convertQueryResultToObject(this.clazz,
					getQueryResultsResponse);
			return list;
		} catch (Exception e) {
			log.warning(e.getMessage());
			return new ArrayList<T>();
		}
	}
}