package spout;

import clojure.lang.Cons;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import util.Constants;
import yahoofinance.Stock;
import yahoofinance.YahooFinance;
import yahoofinance.quotes.fx.FxQuote;
import yahoofinance.quotes.stock.StockQuote;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class StockPriceSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
    /**
     * initialize spout and connection to external sources
     * @param map
     * @param topologyContext
     * @param collector
     */
    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    /**
     * receive data from source and emit to next bolt
     */
    @Override
    public void nextTuple() {
        try {
            StockQuote quote = YahooFinance.get(Constants.AAPL).getQuote();

            BigDecimal price = quote.getPrice();
            BigDecimal prevClose = quote.getPreviousClose();
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());

            collector.emit(new Values(Constants.AAPL, sdf.format(timestamp),
                                    price.doubleValue(), prevClose.doubleValue()));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(this.getOutputFields()));
    }

    private List<String> getOutputFields() {
        List<String> listOfOutputFields = Arrays.asList(
                Constants.COMPANY_NAME_FIELD,
                Constants.TIMESTAMP_FIELD,
                Constants.PRICE_FIELD,
                Constants.PREV_CLOSE_FIELD
        );

        return listOfOutputFields;
    }
}
