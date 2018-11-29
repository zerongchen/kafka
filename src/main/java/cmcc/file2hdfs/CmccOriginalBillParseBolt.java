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

public class CmccOriginalBillParseBolt extends BaseRichBolt {

	private static final Log LOG = LogFactory
			.getLog(CmccOriginalBillParseBolt.class);
	private OutputCollector collector;
	
	public CmccOriginalBillParseBolt(){
		
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void execute(Tuple input) {
		String line = input.getString(0);
		LOG.debug("RECV[kafka -> statDay] " + line.length() + " " + line);
		if (!line.isEmpty()) {
			String[] fields = line.split("\\|");
			if(fields.length>19){
				String statTimestamp = fields[18];
				if(isNumber(statTimestamp) && statTimestamp.length()==13){
					String statDay = "19700101";
					String statHour = "00";
					long stt = Long.parseLong(statTimestamp);
					if(stt + 86400000>System.currentTimeMillis()){
						String dt = DateUtil.getHourStr(stt);
						
						if(dt.length() == 10){
							statDay = dt.substring(0,8);
							statHour = dt.substring(8);
							String billType = fields[0].trim();
						    if(billType != null && billType.length() == 3){
						    	LOG.debug("EMIT[OriginalBillParse -> hdfs] billType=" + billType + ",statDay=" + statDay + ",statHour=" + statHour);
						    	collector.emit(input, new Values(billType,statDay,statHour, line));
						    }
						    else{
								LOG.error("DATA FORMAT ERROR:" + line);
							}
						}
						else{
							LOG.error("DATA FORMAT ERROR:" + line);
						}
					}
					else{
						LOG.warn("DATA TIMEOUT:" + line);
					}
				}
				else{
					LOG.error("DATA FORMAT ERROR:" + line);
				}
			}
			else{
				LOG.error("DATA FORMAT ERROR:" + line);
			}
		}
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("billType","statDay", "statHour","line"));
	}
	
	private boolean isNumber(String num) {
		return (num!=null && num.matches("[0-9]+"));
	}
}
