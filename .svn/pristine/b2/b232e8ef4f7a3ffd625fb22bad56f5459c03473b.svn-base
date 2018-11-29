package cmcc.file2hdfs.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.log4j.PropertyConfigurator;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class CmccConfig {
	
	//#kafka config
	private String kafkaZks;
	private String kafkaBrokerZkPath;
	//#hdfs config
	private String hdfsUri;
	private String hdfsUser;
	//#storm config
	private String stormZks;
	private int stormZkport;
	private String stormZkRoot;
	private int stormWorkerNum;
	private String hiveJdbcUrl;
	private String hiveJdbcDriver;
	private String hiveJdbcUser;
	private String hiveJdbcPasswd;

	private List<Item> fiveMinItemList = new ArrayList<Item>();
	private List<Item> originalBillItemList = new ArrayList<Item>();
	private static CmccConfig instance;

	private CmccConfig() {
		
		PropertyConfigurator.configure(this.getClass().getClassLoader().getResource("conf/log4j.properties"));
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		InputStream is = null;
		try {
			DocumentBuilder db = dbf.newDocumentBuilder();
			
			/*if(configFile != null)
				is = new FileInputStream(configFile); 
			else
				is = this.getClass().getClassLoader().getResourceAsStream("conf/config.xml");
			*/	
			is = this.getClass().getClassLoader().getResourceAsStream("conf/config.xml");
			
			Document doc = db.parse(is);
			
			//#kafka config
			kafkaZks = doc.getElementsByTagName("kafka.zks").item(0).getFirstChild().getNodeValue();
			kafkaBrokerZkPath = doc.getElementsByTagName("kafka.brokerZkPath").item(0).getFirstChild().getNodeValue();
			//#hdfs config
			hdfsUri = doc.getElementsByTagName("hdfs.uri").item(0).getFirstChild().getNodeValue();
			hdfsUser = doc.getElementsByTagName("hdfs.user").item(0).getFirstChild().getNodeValue();
			//#storm config
			stormZks = doc.getElementsByTagName("storm.zks").item(0).getFirstChild().getNodeValue();
			stormZkport = Integer.valueOf(doc.getElementsByTagName("storm.zkport").item(0).getFirstChild().getNodeValue());
			stormZkRoot = doc.getElementsByTagName("storm.zkRoot").item(0).getFirstChild().getNodeValue();
			stormWorkerNum = Integer.valueOf(doc.getElementsByTagName("storm.workerNum").item(0).getFirstChild().getNodeValue());
			//#hive config
			hiveJdbcUrl = doc.getElementsByTagName("hive.jdbc.url").item(0).getFirstChild().getNodeValue();
			hiveJdbcDriver = doc.getElementsByTagName("hive.jdbc.driver").item(0).getFirstChild().getNodeValue();
			hiveJdbcUser = doc.getElementsByTagName("hive.jdbc.user").item(0).getFirstChild().getNodeValue();
			hiveJdbcPasswd = doc.getElementsByTagName("hive.jdbc.passwd").item(0).getFirstChild()==null?"":doc.getElementsByTagName("hive.jdbc.passwd").item(0).getFirstChild().getNodeValue();
			
			NodeList nodeList = doc.getElementsByTagName("item");
			for (int i = 0; i < nodeList.getLength(); i++) {
				Node site = nodeList.item(i);
				Item item = new Item();
				for (Node node = site.getFirstChild(); node != null; 
						node = node.getNextSibling()) {
					if (node.getNodeType() == Node.ELEMENT_NODE) {
						String name = node.getNodeName();
						String value = node.getFirstChild().getNodeValue();
						//System.out.println(name + ":" + value + "\t");
						
						if(name.equals("topic")) item.setTopic(value); 
						else if(name.equals("spout.parallelism_hint")) item.setSpoutParallelism_hint(Integer.parseInt(value));
						else if(name.equals("bolt.parallelism_hint")) item.setBoltParallelism_hint(Integer.parseInt(value));
						else if(name.equals("tablename")) item.setTablename(value);
						else if(name.equals("hdfs.path")) item.setHdfsPath(value);
						else if(name.equals("tick.tuple.interval")) item.setTickTupleInterval(Integer.parseInt(value));
						else if(name.equals("sync.count")) item.setSyncCount(Integer.parseInt(value));
						
					}
				}
				fiveMinItemList.add(item);
			}
			
			nodeList = doc.getElementsByTagName("originalBill");
			for (int i = 0; i < nodeList.getLength(); i++) {
				Node site = nodeList.item(i);
				Item originalBillItem = new Item();
				for (Node node = site.getFirstChild(); node != null; 
						node = node.getNextSibling()) {
					if (node.getNodeType() == Node.ELEMENT_NODE) {
						String name = node.getNodeName();
						String value = node.getFirstChild().getNodeValue();
						//System.out.println(name + ":" + value + "\t");
						
						if(name.equals("topic")) originalBillItem.setTopic(value); 
						else if(name.equals("spout.parallelism_hint")) originalBillItem.setSpoutParallelism_hint(Integer.parseInt(value));
						else if(name.equals("bolt.parallelism_hint")) originalBillItem.setBoltParallelism_hint(Integer.parseInt(value));
						else if(name.equals("tablename")) originalBillItem.setTablename(value);
						else if(name.equals("hdfs.path")) originalBillItem.setHdfsPath(value);
						else if(name.equals("tick.tuple.interval")) originalBillItem.setTickTupleInterval(Integer.parseInt(value));
						else if(name.equals("sync.count")) originalBillItem.setSyncCount(Integer.parseInt(value));
						
					}
				}
				originalBillItemList.add(originalBillItem);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally{
			if(is != null){
				try {
					is.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public synchronized static CmccConfig getInstance() {
		if (instance == null) {
			instance = new CmccConfig();
		}
		return instance;
	}

	private String getSystemWorkPath(){
		String path = System.getProperty("work.dir");
		if(path == null || path.length()==0){
			path = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();//Thread.currentThread().getContextClassLoader().getResource("./").getPath();
			//path = path + "../";
			if(System.getProperty("os.name").toLowerCase().contains("windows")) {
				path = path.replaceFirst("/", "");
			}
			System.setProperty("work.dir", path);
		}
		
		//logger.info(path);
		return path;
	}

	public static class Item{
		private String topic;
		private int spoutParallelism_hint;
		private int boltParallelism_hint;
		private String tablename;
		private String hdfsPath;
		private int tickTupleInterval;
		private int syncCount;
		
		public String getTopic() {
			return topic;
		}
		public int getSpoutParallelism_hint() {
			return spoutParallelism_hint;
		}
		public int getBoltParallelism_hint() {
			return boltParallelism_hint;
		}
		public String getHdfsPath() {
			return hdfsPath;
		}
		public int getTickTupleInterval() {
			return tickTupleInterval;
		}
		public int getSyncCount() {
			return syncCount;
		}
		public void setTopic(String topic) {
			this.topic = topic;
		}
		public void setSpoutParallelism_hint(int spoutParallelism_hint) {
			this.spoutParallelism_hint = spoutParallelism_hint;
		}
		public void setBoltParallelism_hint(int boltParallelism_hint) {
			this.boltParallelism_hint = boltParallelism_hint;
		}
		public void setHdfsPath(String hdfsPath) {
			this.hdfsPath = hdfsPath;
		}
		public void setTickTupleInterval(int tickTupleInterval) {
			this.tickTupleInterval = tickTupleInterval;
		}
		public void setSyncCount(int syncCount) {
			this.syncCount = syncCount;
		}
		public String getTablename() {
			return tablename;
		}
		public void setTablename(String tablename) {
			this.tablename = tablename;
		}
		
	}

	
	public String getKafkaZks() {
		return kafkaZks;
	}

	public String getKafkaBrokerZkPath() {
		return kafkaBrokerZkPath;
	}

	public String getHdfsUri() {
		return hdfsUri;
	}

	public String getHdfsUser() {
		return hdfsUser;
	}

	public String getStormZks() {
		return stormZks;
	}

	public int getStormZkport() {
		return stormZkport;
	}

	public String getStormZkRoot() {
		return stormZkRoot;
	}

	public int getStormWorkerNum() {
		return stormWorkerNum;
	}

	public List<Item> getFiveMinItemList() {
		return fiveMinItemList;
	}
	
	public String getHiveJdbcUrl() {
		return hiveJdbcUrl;
	}

	public String getHiveJdbcDriver() {
		return hiveJdbcDriver;
	}

	public String getHiveJdbcUser() {
		return hiveJdbcUser;
	}

	public String getHiveJdbcPasswd() {
		return hiveJdbcPasswd;
	}
	
	public List<Item> getOriginalBillItemList() {
		return originalBillItemList;
	}

	public static void main(String[] args) {
		CmccConfig.getInstance();
	}
}
