package storm.blueprints.chapter1.v1;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.blueprints.utils.Utils;

@SuppressWarnings("serial")
public class SentenceSpout extends BaseRichSpout {
	
	private SpoutOutputCollector collector;
	private String[] sentences = {
			"my dog has fleas",
	        "i like cold beverages",
	        "the dog ate my homework",
	        "don't have a cow man",
	        "i don't think i like fleas"
	};
	private int index = 0;

	
	/**
	 * Initialization
	 * Storm组件在初始化时会调用该方法，三个参数：
	 * Map对象包含了Storm配置信息
	 * TopologyContext对象提供了topology中组件的信息
	 * SpoutOutputCollector对象提供了发射tuple的方法
	 * 
	 * this example，我们在初始化没有做任何额外的操作，仅仅将传入的SpoutOutputCollector对象保存至变量
	 */
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	/**
	 * Core
	 * spout实现的核心，Storm通过调用该方法向输出的collector发射tuple
	 * here：我们发射当前索引对应的语句，并递增索引指向下一个语句
	 */
	public void nextTuple() {
		this.collector.emit(new Values(sentences[index]));
		index++;
		if (index == sentences.length) {
			index = 0;
		}
		Utils.waitForMillis(1);
	}
	
	/**
	 * OutputStream
	 * Storm的组件通过这个方法告诉Storm该组件会发射哪些数据流，每个数据流的tuple包含哪些字段
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}
	
}
