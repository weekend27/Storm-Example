package storm.blueprints.chapter1.v1;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class SplitSentenceBolt extends BaseRichBolt{
	
	private OutputCollector collector;

	/**
	 * Iniatialization
	 * prepare方法在IBolt中定义，类同与ISpout接口中定义的open()方法。
	 * prepare方法在bolt初始化时调用，可以用来准备bolt用到的资源，如数据库连接。
	 */
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	/**
	 * Core
	 * execute方法由IBolt接口定义，每当从订阅的数据流中接收一个tuple，都会调用该方法
	 */
	public void execute(Tuple input) {
		// 订阅sentence字段的值
		String sentence = input.getStringByField("sentence");
		// 根据空格分割单词
		String[] words = sentence.split(" ");
		// 每个单词向后面的输出流发射一个tuple
		for (String word : words) {
			this.collector.emit(new Values(word));
		}
	}

	/**
	 * OutputStream
	 * declare the outputstream
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 声明了一个输出流，每个tuple包含一个字段“word”
		declarer.declare(new Fields("word"));
	}
}
