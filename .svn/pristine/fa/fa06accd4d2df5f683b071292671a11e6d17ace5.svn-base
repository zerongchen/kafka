package cmcc.file2hdfs.utils;

public class FastDateUtil {
	
	private static long[][] hourArr = new long[2880][2];
	private static long[][] dayArr = new long[120][2];
	private static FastDateUtil instance;
	
	private FastDateUtil(){
		initData();
	}

	public synchronized static FastDateUtil getInstance() {
		if (instance == null) {
			instance = new FastDateUtil();
		}
		return instance;
	}

	public synchronized void initData(){
		long ts = System.currentTimeMillis() + 3600000 *72;
		String maxDay = DateUtil.getDayStr(ts);
		String maxHour = maxDay + "00";
		//System.out.println(maxHour);
		long maxTs = DateUtil.hourToStamp(maxHour);
		//System.out.println(maxHour);
		
		hourArr[0][0] = maxTs;
		hourArr[0][1] = Long.parseLong(maxHour);
		int len = hourArr.length;
		for(int i=1;i<len;i++){
			hourArr[i][0] = hourArr[i-1][0]-3600000;
			hourArr[i][1] = Long.parseLong(DateUtil.getHourStr(hourArr[i][0]));
		}
		
		dayArr[0][0] = DateUtil.dayToStamp(maxDay);
		dayArr[0][1] = Long.parseLong(maxDay);
		len = dayArr.length;
		for(int i=1;i<len;i++){
			dayArr[i][0] = hourArr[i-1][0]-86400000;
			dayArr[i][1] = Long.parseLong(DateUtil.getDayStr(dayArr[i][0]));
		}
		
	}
	
	public long getHour(long timestamp) {
		int len = hourArr.length;
		for(int i=0;i<len;i++){
			if(timestamp>hourArr[i][0]){
				return hourArr[i][1];
			}
		}
		return Long.parseLong(DateUtil.getHourStr(timestamp));
	}
	
	public long getDay(long timestamp) {
		int len = dayArr.length;
		for(int i=0;i<len;i++){
			if(timestamp>dayArr[i][0]){
				return dayArr[i][1];
			}
		}
		return Long.parseLong(DateUtil.getDayStr(timestamp));
	}
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		FastDateUtil fdu = FastDateUtil.getInstance();
		
		System.out.println(fdu.getHour(System.currentTimeMillis()));
		System.out.println(fdu.getDay(1480325276508l));
		
		long s_ts = System.currentTimeMillis();
		for(int v=0;v<10000000;v++){
			long timestamp = System.currentTimeMillis();
			
			long hour = fdu.getHour(timestamp);
			//System.out.println(hour);
			
		}
		System.out.println(System.currentTimeMillis()-s_ts);
		
		
		s_ts = System.currentTimeMillis();
		for(int v=0;v<10000000;v++){
			long timestamp = System.currentTimeMillis();
			String s = DateUtil.getHourStr(timestamp);
			//System.out.println(s);
			
		}
		System.out.println(System.currentTimeMillis()-s_ts);

	}

}
