package cmcc.file2hdfs;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;

import cmcc.file2hdfs.utils.HiveUtil;

public class CmccFiveMinHdfsBolt extends BaseRichBolt {

	private static final Log LOG = LogFactory.getLog(CmccFiveMinHdfsBolt.class);
	private OutputCollector collector;

	protected transient Object writeLock;
	private Map<Integer, List<String>> dataCacheMap = null;
	private Map<Integer, Integer> countCacheMap = null;

	protected int syncCount = 1000;
	protected int tickTupleInterval = 300;
	private String hdfsPath;
	private String hdfsUri;
	private String hdfsUser;
	private String fileNamePrefix;
	private String tablename;

	public CmccFiveMinHdfsBolt(String hdfsUri, String hdfsUser, String hdfsPath,
			String tablename,String fileNamePrefix, int tickTupleInterval, int syncCount) {
		this.hdfsUri = hdfsUri;
		this.hdfsUser = hdfsUser;
		this.hdfsPath = hdfsPath;
		this.tablename = tablename;
		this.fileNamePrefix = fileNamePrefix;
		this.tickTupleInterval = tickTupleInterval;
		this.syncCount = syncCount;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.dataCacheMap = new ConcurrentHashMap<Integer, List<String>>();
		this.countCacheMap = new ConcurrentHashMap<Integer, Integer>();
		this.writeLock = new Object();
	}

	@Override
	public void execute(Tuple input) {
		boolean forceSync = false;
		int syncStatDay = 0;
		if (TupleUtils.isTick(input)) {
			forceSync = true;
		} else {
			int statDay = input.getIntegerByField("statDay");
			String line = input.getStringByField("line");
			if (dataCacheMap.containsKey(statDay)) {
				dataCacheMap.get(statDay).add(line);
			} else {
				List<String> list = new ArrayList<String>();
				list.add(line);
				dataCacheMap.put(statDay, list);
			}
			// count
			if (countCacheMap.containsKey(statDay)) {
				Integer cnt = countCacheMap.get(statDay);
				cnt++;
				if (cnt < syncCount) {
					countCacheMap.put(statDay, cnt);
				} else {
					countCacheMap.remove(statDay);
					forceSync = true;
					syncStatDay = statDay;
				}
			} else {
				countCacheMap.put(statDay, 1);
			}
			collector.ack(input);
		}
		// write data
		if (forceSync) {
			if (syncStatDay != 0) {
				List<String> list = dataCacheMap.get(syncStatDay);

				// output hdfs
				new Writer(hdfsUri, hdfsUser, hdfsPath, fileNamePrefix,
						syncStatDay, list,tablename).start();

				dataCacheMap.remove(syncStatDay);
			} else {
				for (Integer statDay : dataCacheMap.keySet()) {
					List<String> list = dataCacheMap.get(statDay);

					// output hdfs
					new Writer(hdfsUri, hdfsUser, hdfsPath, fileNamePrefix,
							statDay, list,tablename).start();

					dataCacheMap.remove(statDay);
					countCacheMap.remove(statDay);
				}
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		if(tickTupleInterval>0){
			Map<String, Object> map = new HashMap<String, Object>();
			// 指定多长时间响应一次事件
			map.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickTupleInterval);
			return map;
		}
		else{
			return null;
		}
		// return
		// TupleUtils.putTickFrequencyIntoComponentConfig(super.getComponentConfiguration(),
		// tickTupleInterval);
	}

	public static class Writer extends Thread {
		String hdfsUri;
		String hdfsUser;
		String hdfsPath;
		String fileNamePrefix;
		int statDay;
		List<String> list;
		String tablename;
		
		public Writer(String hdfsUri,String hdfsUser,String hdfsPath,String fileNamePrefix,
				int statDay,List<String> list,String tablename){
			this.hdfsUri = hdfsUri;
			this.hdfsUser = hdfsUser;
			this.hdfsPath = hdfsPath;
			this.fileNamePrefix = fileNamePrefix;
			this.statDay = statDay;
			this.list = list;
			this.tablename = tablename;
		}
		
		@Override
		public void run() {
	
			if(list == null || list.size() == 0 )
				return;
			
			FSDataOutputStream out = null;
			FileSystem fs = null;
			try {
				Configuration conf = new Configuration();
				conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
				conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));
				conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
				conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
				fs = FileSystem.get(conf);
				
				//fs = FileSystem.get(new URI(hdfsUri),new Configuration(), hdfsUser);
	
				Path path = new Path(hdfsPath + "partdate=" + statDay);
				if (!fs.exists(path)) {
					int retcode = createHiveTablePartition(statDay);
					if(retcode != 0){
						fs.mkdirs(path);
					}
				}
	
				String currDatetime = UUID.randomUUID().toString();//DateUtil.getCurrDateTime();
				//int rnd = new Random().nextInt(10000);
				//first,write a hidden file
				String writingFilename = hdfsPath + "partdate=" + statDay + "/."
						+ fileNamePrefix + statDay + "_" + currDatetime
						+ ".txt";
				Path writingfile = new Path(writingFilename);
				out = fs.create(writingfile);
				
				if(fs.exists(writingfile)){
					LOG.info("Starting to write file: " + writingFilename);
					
					for (String line : list) {
						out.writeBytes(line + "\r\n");
					}
		
					out.close();
					out = null;
					LOG.info("Finished writing file: " + writingFilename);
					//rename
					String dstFilename = hdfsPath + "partdate=" + statDay + "/"
							+ fileNamePrefix + statDay + "_" + currDatetime
							 + ".txt";
					fs.rename(writingfile, new Path(dstFilename));
					LOG.info("Renamed file: " + writingFilename + " to " + dstFilename);
				}
				else{
					LOG.error("File create fail: " + writingFilename + "," +  list.size() + " records lost");
				}

				list.clear();
				list = null;
	
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				if (out != null) {
					try {
						out.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					out = null;
				}
			}
		}
		
		private int createHiveTablePartition(int statDay){
			Connection connection = HiveUtil.getHiveConnection();
			String sql = "alter table " + tablename + " add if not exists partition (partdate=" + statDay + ") location 'partdate=" + statDay + "'";
			Statement stmt = null;
			try {
				stmt = connection.createStatement();
				stmt.execute(sql);
				return 0;
			} catch (SQLException e) {
				e.printStackTrace();
			}
			finally{
				if(stmt != null){
					try {
						stmt.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					stmt = null;
				}
			}
			return 1;
		}
	}

}
