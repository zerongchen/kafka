package cmcc.file2hdfs;

import java.util.Arrays;
import java.util.List;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import cmcc.file2hdfs.utils.CmccConfig;
import cmcc.file2hdfs.utils.CmccConfig.Item;


public class CmccFiveMinDataStoreTopo {

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, InterruptedException,
			AuthorizationException {

		CmccConfig config = CmccConfig.getInstance();
		
		// Configure Kafka
		String zks = config.getKafkaZks();
		BrokerHosts brokerHosts = new ZkHosts(zks,config.getKafkaBrokerZkPath());

		//
		TopologyBuilder builder = new TopologyBuilder();
		List<Item> list = config.getFiveMinItemList();
		for(Item item:list){
			String topic = item.getTopic();
			String spoutId = "id_" + topic;
			
			String zkRoot = config.getStormZkRoot(); // default zookeeper root configuration for storm
			SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);
			spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
			// spoutConf.forceFromStart = true;
			spoutConf.zkServers = Arrays.asList(config.getStormZks().split(","));
			spoutConf.zkPort = config.getStormZkport();
	
			// configure & build topology
			builder.setSpout(topic + "-kafka-reader", new KafkaSpout(spoutConf), item.getSpoutParallelism_hint());
			builder.setBolt(topic + "-parseData", new CmccFiveMinParseBolt(), item.getBoltParallelism_hint())
					.shuffleGrouping(topic + "-kafka-reader");
			CmccFiveMinHdfsBolt hdfsBolt = 
					new CmccFiveMinHdfsBolt(config.getHdfsUri(),
									config.getHdfsUser(), 
									item.getHdfsPath(),
									item.getTablename(),
									item.getTopic() + "_5min_",
									item.getTickTupleInterval(),
									item.getSyncCount());
			builder.setBolt(topic + "HdfsBolt", hdfsBolt, Math.round(item.getBoltParallelism_hint()*1.2))
					.fieldsGrouping(topic + "-parseData", new Fields("statDay"));
		}

		// submit topology
		Config conf = new Config();		
		
		String name = CmccFiveMinDataStoreTopo.class.getSimpleName();
		if (args != null && args.length > 0) {
			name = args[0];
			//conf.put(Config.NIMBUS_HOST, nimbus);
			conf.setNumWorkers(config.getStormWorkerNum());
			StormSubmitter.submitTopologyWithProgressBar(name, conf,
					builder.createTopology());
		} else {
			System.setProperty("hadoop.home.dir", "D:\\hadoop-win");
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(name, conf, builder.createTopology());
			Thread.sleep(600000);
			cluster.shutdown();
		}

	}

}
