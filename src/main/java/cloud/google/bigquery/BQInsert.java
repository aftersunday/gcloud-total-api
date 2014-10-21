package cloud.google.bigquery;

import java.io.IOException;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import cloud.google.annotation.Annotation.BigQuery_Id;
import cloud.google.annotation.Annotation.BigQuery_Remove;
import cloud.google.util.Utility;

import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest.Rows;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableRow;

public class BQInsert {

	private static final Logger log = Logger
			.getLogger(BQInsert.class.getName());

	public static <T> boolean insert(T obj) {
		Rows rows = convertObjectToTableRows(obj);
		List<Rows> rowList = new ArrayList<Rows>();
		rowList.add(rows);

		TableDataInsertAllRequest content = new TableDataInsertAllRequest()
				.setRows(rowList);
		try {
			TableDataInsertAllResponse response = BQConfig
					.getBigquery()
					.tabledata()
					.insertAll(BQConfig.PROJECT_ID, BQConfig.DATASET_ID,
							obj.getClass().getSimpleName(), content).execute();
			log.info(response.toPrettyString());
			return !(response.getInsertErrors() != null && response
					.getInsertErrors().size() > 0);
		} catch (IOException e) {
			log.severe(e.getMessage());
			return false;
		}
	}

	public static <T> boolean insert(T... objs) {
		List<Rows> rowList = new ArrayList<Rows>();
		Class<?> type = null;
		for (T object : objs) {
			type = object.getClass();
			Rows rows = convertObjectToTableRows(object);
			rowList.add(rows);
		}

		TableDataInsertAllRequest content = new TableDataInsertAllRequest()
				.setRows(rowList);
		try {
			TableDataInsertAllResponse response = BQConfig
					.getBigquery()
					.tabledata()
					.insertAll(BQConfig.PROJECT_ID, BQConfig.DATASET_ID,
							type.getSimpleName(), content).execute();
			log.info(response.toPrettyString());
			return !(response.getInsertErrors() != null && response
					.getInsertErrors().size() > 0);
		} catch (IOException e) {
			log.severe(e.getMessage());
			return false;
		}
	}

	public static <T> boolean insert(List<T> objs) {
		List<Rows> rowList = new ArrayList<Rows>();
		Class<?> type = null;
		for (T object : objs) {
			type = object.getClass();
			Rows rows = convertObjectToTableRows(object);
			rowList.add(rows);
		}

		TableDataInsertAllRequest content = new TableDataInsertAllRequest()
				.setRows(rowList);
		try {
			TableDataInsertAllResponse response = BQConfig
					.getBigquery()
					.tabledata()
					.insertAll(BQConfig.PROJECT_ID, BQConfig.DATASET_ID,
							type.getSimpleName(), content).execute();
			log.info(response.toPrettyString());
			return !(response.getInsertErrors() != null && response
					.getInsertErrors().size() > 0);
		} catch (IOException e) {
			log.severe(e.getMessage());
			return false;
		}
	}

	private static <T> Rows convertObjectToTableRows(T obj) {
		TableDataInsertAllRequest.Rows rows = new TableDataInsertAllRequest.Rows();
		TableRow row = new TableRow();
		String objectId = "";
		for (Field f : obj.getClass().getDeclaredFields()) {
			f.setAccessible(true);
			String fName = f.getName();
			String fType = f.getType().getName();
			Object fValue = null;

			if (f.isAnnotationPresent(BigQuery_Remove.class)) {
				continue;
			}
			if (f.isAnnotationPresent(BigQuery_Id.class)) {
				try {
					if (f.get(obj) != null) {
						objectId = (String) f.get(obj);
					}
				} catch (Exception e) {
					log.severe(e.getMessage());
					objectId = "";
				}
			}
			if (Utility.isStringField(fType)) {
				try {
					if (f.get(obj) != null) {
						fValue = (String) f.get(obj);
					}
				} catch (Exception e) {
					log.severe(e.getMessage());
					fValue = "";
				}
			} else if (Utility.isIntegerField(fType)) {
				try {
					if (f.get(obj) != null) {
						fValue = f.getInt(obj);
					}
				} catch (Exception e) {
					log.severe(e.getMessage());
					fValue = 0;
				}
			} else if (Utility.isDateTimeField(fType)) {
				SimpleDateFormat dateFormat = new SimpleDateFormat(
						"YYYY-MM-dd HH:mm:ss.SSS");
				try {
					if (f.get(obj) != null) {
						Date date = (Date) f.get(obj);
						fValue = dateFormat.format(date);
					}
				} catch (Exception e) {
					log.severe(e.getMessage());
					Date date = Calendar.getInstance().getTime();
					fValue = dateFormat.format(date);
				}
			} else if (Utility.isBooleanField(fType)) {
				try {
					if (f.get(obj) != null) {
						fValue = (Boolean) f.get(obj);
					}
				} catch (Exception e) {
					log.severe(e.getMessage());
					fValue = true;
				}
			} else if (Utility.isDoubleField(fType)
					|| Utility.isLongField(fType)) {
				try {
					if (f.get(obj) != null) {
						fValue = (Double) f.get(obj);
					}
				} catch (Exception e) {
					log.severe(e.getMessage());
					fValue = 0.0;
				}
			}
			row.set(fName, fValue);
		}
		rows.setInsertId(objectId);
		rows.setJson(row);
		return rows;
	}
}