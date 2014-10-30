package cloud.google.bigquery;

public class BQQuery<T> {

	public BQQueryObject<T> query(Class<T> clazz) {
		return new BQQueryObject<T>(clazz);
	}

	public BQQueryString<T> cmdQuery(Class<T> clazz) {
		return new BQQueryString<T>(clazz);
	}

}