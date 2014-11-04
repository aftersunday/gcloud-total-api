package cloud.google.bigquery;

public class BQQuery<T> {

	public BQConfig config;

	public BQQuery(BQConfig config) {
		this.config = config;
	}

	public BQObjectQuery<T> query(Class<T> clazz) {
		return new BQObjectQuery<T>(this.config, clazz);
	}

	public BQSQLQuery<T> sqlQuery(Class<T> clazz) {
		return new BQSQLQuery<T>(this.config, clazz);
	}

}