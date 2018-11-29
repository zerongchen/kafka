package cmcc.file2hdfs;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import cmcc.file2hdfs.utils.DateUtil;

public class CmccFiveMinParseBolt extends BaseRichBolt {

	private static final Log LOG = LogFactory.getLog(CmccFiveMinParseBolt.class);
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String line = input.getString(0).trim();
		LOG.debug("RECV[kafka -> statDay] " + line.length() + " " + line);
		int idx = line.indexOf("|");
		if (!line.isEmpty() && idx>0) {
			String statTimestamp = line.substring(0,idx);
			if(statTimestamp.matches("[0-9]+") && statTimestamp.length() ==10){
				int statDay = DateUtil.getDay(Long.parseLong(statTimestamp + "000"));
				LOG.info("EMIT[statDay -> hdfs] " + statDay);
				collector.emit(input, new Values(statDay, line));
			}
			else{
				LOG.error("DATA FORMAT ERROR:" + line);
			}
		}
		else{
			LOG.error("DATA FORMAT ERROR:" + line);
		}
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("statDay", "line"));
	}
	
	

}
