package cloud.google.util;

import java.io.UnsupportedEncodingException;
import java.util.Calendar;
import java.util.Date;

public class StringHelper {

	private static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	private static final long BASE = 36;

	public static Date removeTime(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTime();
	}

	public static String getId() {
		Date date = new Date();
		String id = encode(date.getTime());
		return id;
	}

	public static String encode(long num) {
		StringBuilder sb = new StringBuilder();
		while (num > 0) {
			sb.append(ALPHABET.charAt((int) (num % BASE)));
			num /= BASE;
		}
		return sb.reverse().toString();
	}

	public static String cleanContent(String str) {
		str = str.replaceAll("width[ ]*:[ ]*[0-9a-zA-Z ]+[;]*", "");
		return str;
	}

	public static String replaceSpace(String str) {
		str = str.replaceAll("[\\'\\/]+", "");
		str = str
				.replaceAll(
						"[-\\!\"\\@\\#\\,\\.\\$%\\^&\\*\\(\\)_\\+\\=\\?\\;\\:\\~\\`\\{\\}\\[\\]\\|\\\\]+",
						"-");
		str = str.replaceAll(" ", "-");
		str = str.replaceAll("[-]+", "-");
		str = str.replaceAll("^-", "");
		str = str.replaceAll("-$", "");
		str = str.toLowerCase();
		return str;
	}

	public static String getUTF8FromString(String input) {
		String result = "";
		try {
			result = new String(input.getBytes(("ISO-8859-1")), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return result;
	}
}
