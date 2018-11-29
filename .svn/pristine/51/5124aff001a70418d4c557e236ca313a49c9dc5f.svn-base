package cmcc.file2hdfs;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;

import cmcc.file2hdfs.utils.HiveUtil;

public class CmccOriginalBillHdfsBolt extends BaseRichBolt {

	private static final Log LOG = LogFactory.getLog(CmccOriginalBillHdfsBolt.class);
	private OutputCollector collector;

	protected transient Object writeLock;
	private Map<String, List<String>> dataCacheMap = null;
	private Map<String, Integer> countCacheMap = null;

	private int syncCount = 1000;
	private int tickTupleInterval = 300;
	private String hdfsPath;
	private String hdfsUri;
	private String hdfsUser;
	private String fileNamePrefix;
	private String tablename;

	public CmccOriginalBillHdfsBolt(String hdfsUri, String hdfsUser, String hdfsPath,
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
		this.dataCacheMap = new ConcurrentHashMap<String, List<String>>();
		this.countCacheMap = new ConcurrentHashMap<String, Integer>();
		this.writeLock = new Object();
	}

	@Override
	public void execute(Tuple input) {
		boolean forceSync = false;
		String syncKey = "";
		if (TupleUtils.isTick(input)) {
			forceSync = true;
		} else {
			String statDay = input.getStringByField("statDay");
			String statHour = input.getStringByField("statHour");
			String billType = input.getStringByField("billType");
			String line = input.getStringByField("line");
			String key = String.format("%s_%s_%s", billType,statDay,statHour);
			if (dataCacheMap.containsKey(key)) {
				dataCacheMap.get(key).add(line);
			} else {
				List<String> list = new ArrayList<String>();
				list.add(line);
				dataCacheMap.put(key, list);
			}
			// count
			if (countCacheMap.containsKey(key)) {
				Integer cnt = countCacheMap.get(key);
				cnt++;
				if (cnt < syncCount) {
					countCacheMap.put(key, cnt);
				} else {
					
					forceSync = true;
					syncKey = key;
				}
			} else {
				countCacheMap.put(key, 1);
			}
			collector.ack(input);
		}
		// write data
		if (forceSync) {
			if (syncKey != null && syncKey.length()>0) {
				List<String> list = dataCacheMap.get(syncKey);

				// output hdfs
				new Writer(hdfsUri, hdfsUser, hdfsPath, fileNamePrefix,
						syncKey, list,tablename).start();
				
				countCacheMap.remove(syncKey);
				dataCacheMap.remove(syncKey);

			} else {
				for (String key : dataCacheMap.keySet()) {
					List<String> list = dataCacheMap.get(key);

					// output hdfs
					new Writer(hdfsUri, hdfsUser, hdfsPath, fileNamePrefix,
							key, list,tablename).start();
					

					dataCacheMap.remove(key);
					countCacheMap.remove(key);
					
				}
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> map = new HashMap<String, Object>();
		// 指定多长时间响应一次事件
		map.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickTupleInterval);
		return map;
		// return
		// TupleUtils.putTickFrequencyIntoComponentConfig(super.getComponentConfiguration(),
		// tickTupleInterval);
	}

	

	public static class Writer extends Thread {
		
		String hdfsUri;
		String hdfsUser;
		String hdfsPath;
		String fileNamePrefix; 
		String key; 
		List<String> list;
		String tablename;
		
		public Writer(String hdfsUri, String hdfsUser, String hdfsPath,
				String fileNamePrefix, String key, List<String> list,String tablename){
			this.hdfsUri = hdfsUri;
			this.hdfsUser = hdfsUser;
			this.hdfsPath = hdfsPath;
			this.fileNamePrefix = fileNamePrefix;
			this.key = key;
			this.list = list;
			this.tablename = tablename;
		}
		
		@Override
		public void run() {
			if(list == null || list.size() == 0 )
				return;
			String[] fields = key.split("_");
			String billType = fields[0];
			String statDay = fields[1];
			String statHour = fields[2];
			
			
			FSDataOutputStream out = null;
			FileSystem fs = null;
			try {
				Configuration conf = new Configuration();
				conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
				conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));
				conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
				conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
				conf.set("dfs.replication", "1");
				//if(billType.equals("100") || billType.equals("103")){
				//	conf.set("dfs.blocksize", "");
				//	conf.set("dfs.block.size", "");
				//}
				fs = FileSystem.get(conf);
				
				//fs = FileSystem.get(new URI(hdfsUri),new Configuration(), hdfsUser);

				String path = hdfsPath + "billtype=" + billType + "/partdate=" + statDay + "/hour=" + statHour;
				Path dir = new Path(path);
				if (!fs.exists(dir)) {
					//创建分区
					int retcode = createHiveTablePartition(billType,statDay,statHour);
					if(retcode !=0){
						fs.mkdirs(dir);
						LOG.info("The table partition created fail,make dir[" + path + "].");
					}
				}

				String currDatetime = UUID.randomUUID().toString();//DateUtil.getCurrDateTime();
				//int rnd = new Random().nextInt(10000);
				//first,write a hidden file
				String writingFilename = path + "/."+ fileNamePrefix + statDay 
						+ "_" + currDatetime + ".txt";
				Path writingfile = new Path(writingFilename);
				out = fs.create(writingfile);
				
				if(fs.exists(writingfile)){
					
					LOG.info("Starting to write file: " + writingFilename);
					
					/*for (String line : list) {
						out.writeBytes(line + "\r\n");
					}
					int cnt = 0;*/
					while(list.size()>0){
					    out.writeBytes(list.remove(0) + "\r\n");
					    /*cnt ++;
					    if(cnt >=10000){
					    	if (out instanceof HdfsDataOutputStream) {
								((HdfsDataOutputStream) out).hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
						    } else {
						    	out.hsync();
						    }
					    	cnt = 0;
					    }*/
					}
					
					out.close();
					out = null;
					LOG.info("Finished writing file: " + writingFilename);
	
					//rename
					String dstFilename = path + "/" + fileNamePrefix + statDay 
							+ "_" + currDatetime + ".txt";
					fs.rename(writingfile, new Path(dstFilename));
					LOG.info("Renamed file: " + writingFilename + " to " + dstFilename);
				}
				else{
					LOG.error("File create fail: " + writingFilename + "," +  list.size() + " records lost");
				}
				
				list.clear();
				list = null;

			} catch (Exception e) {
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
		
		private int createHiveTablePartition(String billType,String statDay,String statHour){
			Connection conn = HiveUtil.getHiveConnection();
			String sql = "alter table " + tablename + " add if not exists partition (billtype = " + billType + ",partdate=" + statDay + ",hour=" + statHour + ") location 'billtype=" + billType + "/partdate=" + statDay + "/hour=" + statHour + "'";
			Statement stmt = null;
			try {
				stmt = conn.createStatement();
				stmt.execute(sql);
				LOG.info("Excute sql[" + sql + "] successfully.");
				return 0;
			} catch (SQLException e) {
				e.printStackTrace();
				LOG.error("Excute sql[" + sql + "] fail.");
			}
			finally{
				HiveUtil.closeConnection(conn, stmt);
			}
			return 1;
		}
	}
	

}
