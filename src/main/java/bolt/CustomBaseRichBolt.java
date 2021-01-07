package bolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public abstract class CustomBaseRichBolt extends BaseRichBolt {


    private OutputCollector collector;
    private Map<String, Object> topologyConfig;
    private TopologyContext context;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple input) {
        this.executeTask(input);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        this.declareOutputFieldsTask(outputFieldsDeclarer);
    }

    protected TopologyContext getContext() {
        return context;
    }

    protected OutputCollector getCollector() {
        return collector;
    }

    protected abstract void executeTask(Tuple input);

    protected abstract void prepareTask();

    protected abstract void declareOutputFieldsTask(OutputFieldsDeclarer declarer);
}
