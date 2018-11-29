package cmcc.file2hdfs.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
	
	public static int getDay(long timestamp) {
		SimpleDateFormat dateFormater = new SimpleDateFormat("yyyyMMdd");
		Date date = new Date(timestamp);
		return Integer.parseInt(dateFormater.format(date));
	}

	public static String getMonthStr(long timestamp) {
		SimpleDateFormat dateFormater = new SimpleDateFormat("yyyyMM");
		Date date = new Date(timestamp);
		return dateFormater.format(date);
	}

	public static String getDayStr(long timestamp) {
		SimpleDateFormat dateFormater = new SimpleDateFormat("yyyyMMdd");
		Date date = new Date(timestamp);
		return dateFormater.format(date);
	}

	public static String getHourStr(long timestamp) {
		SimpleDateFormat dateFormater = new SimpleDateFormat("yyyyMMddHH");
		Date date = new Date(timestamp);
		return dateFormater.format(date);
	}
	
	

	public static String getCurrDateTime() {
		SimpleDateFormat dateFormater = new SimpleDateFormat(
				"yyyyMMddHHmmssSSS");
		Date date = new Date();
		return dateFormater.format(date);
	}

	public static long hourToStamp(String s){
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHH");
		try {
			Date date = simpleDateFormat.parse(s);
			return date.getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}

		return 0l;
	}

	public static long dayToStamp(String s){
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
		try {
			Date date = simpleDateFormat.parse(s);
			return date.getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}

		return 0l;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.out.println(DateUtil.getHourStr(1479201549000l));
		System.out.println(1479201549000l);
		System.out.println(hourToStamp("2016111517"));
		System.out.println(System.currentTimeMillis());
	}

}
