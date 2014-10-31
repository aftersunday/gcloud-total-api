package cloud.google.bigquery;

public class BQQuery<T> {

	protected Class<T> clazz;
	protected BQConfig config;

	public BQQuery(BQConfig config) {
		this.config = config;
	}

	public BQObjectQuery<T> query(Class<T> clazz) {
		this.clazz = clazz;
		return new BQObjectQuery<T>(this.config);
	}

	public BQSQLQuery<T> sqlQuery(Class<T> clazz) {
		this.clazz = clazz;
		return new BQSQLQuery<T>(this.config);
	}

}