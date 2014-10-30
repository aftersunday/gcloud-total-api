package cloud.google.bigquery;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import cloud.google.util.Utility;

import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.JobReference;

public class BQQueryObject<T> {

	private static final Logger log = Logger.getLogger(BQQueryObject.class
			.getName());

	private int offset = -1;
	private int limit = -1;
	private List<String> projection;
	private List<String> filter;
	private List<String> order;
	private Class<T> clazz;

	public BQQueryObject(Class<T> clazz) {
		this.clazz = clazz;
	}

	public BQQueryObject<T> offset(int offset) {
		this.offset = offset;
		return this;
	}

	public BQQueryObject<T> limit(int limit) {
		this.limit = limit;
		return this;
	}

	public BQQueryObject<T> projection(String projection) {
		if (this.projection == null || this.projection.size() == 0) {
			this.projection = new ArrayList<String>();
		}
		this.projection.add(projection);
		return this;
	}

	public BQQueryObject<T> filter(String field, Object value,
			FilterOperator operator) {
		if (this.filter == null || this.filter.size() == 0) {
			this.filter = new ArrayList<String>();
		}
		String fil = field + " " + operator.getSymbol() + " ";
		String gType = Utility.generateGoogleDataType(value.getClass()
				.toString());
		if (gType.equals("doubleValue") || gType.equals("booleanValue")
				|| gType.equals("integerValue")) {
			fil += value;

		} else if (gType.equals("dateTimeValue") || gType.equals("stringValue")) {
			fil += "'" + value + "'";
		}
		this.filter.add(fil);
		return this;
	}

	public BQQueryObject<T> order(String field, OrderDirection direction) {
		if (this.order == null || this.order.size() == 0) {
			this.order = new ArrayList<String>();
		}
		String ord = field + " " + direction.name();
		this.order.add(ord);
		return this;
	}

	public List<T> list() {
		String strQuery = "SELECT ";
		if (this.projection != null && this.projection.size() > 0) {
			for (String pr : this.projection) {
				strQuery += pr + ",";
			}
			strQuery = strQuery.substring(0, strQuery.length() - 1) + " ";

		} else {
			strQuery += "* ";
		}
		if (this.clazz != null) {
			strQuery += "FROM " + BQConfig.DATASET_ID + "."
					+ this.clazz.getSimpleName() + " ";
		}
		if (filter != null && filter.size() > 0) {
			strQuery += "WHERE ";
			for (int i = 0; i < this.filter.size(); i++) {
				strQuery += this.filter.get(i) + " AND ";
			}
			strQuery = strQuery.substring(0, strQuery.length() - 5) + " ";
		}
		if (order != null && order.size() > 0) {
			strQuery += "ORDER BY ";
			for (int i = 0; i < this.order.size(); i++) {
				strQuery += this.order.get(i) + " ";
			}
		}
		if (offset != -1) {
			strQuery += "OFFSET " + this.offset + " ";
		}
		if (limit != -1) {
			strQuery += "LIMIT " + this.limit;
		}
		System.out.println(strQuery);
		try {
			JobReference jobReference = BQCoreFunction.createJob(strQuery);
			BQCoreFunction.checkQueryResults(jobReference.getJobId());
			GetQueryResultsResponse getQueryResultsResponse = BQCoreFunction
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

	public static void main(String[] args) {
		Object a = 1;
		System.out.println(a.getClass().getSimpleName());
	}
}