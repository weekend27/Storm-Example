package storm.blueprints.chapter1.v1;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("serial")
public class WordCountBolt extends BaseRichBolt{
	
	private OutputCollector collector;
	private HashMap<String, Long> counts = null;

	/**
	 * Initialization
	 */
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.counts = new HashMap<String, Long>();
	}

	/**
	 * Core
	 */
	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		Long count = this.counts.get(word);
		// 如果接收到的单词没有出现过则初始化为0
		if (count == null) {
			count = 0L;
		}
		// 递增
		count++;
		this.counts.put(word, count);
		this.collector.emit(new Values(word, count));
	}

	/**
	 * OutputStream
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}
    
}
