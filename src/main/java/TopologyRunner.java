import bolt.ComputeGainSignalBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import spout.StockPriceSpout;
import util.Constants;

public class TopologyRunner {
    public static void main(String[] args) {

        // Build Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(Constants.STOCK_PRICE_TRACKER_SPOUT, new StockPriceSpout());
        builder.setBolt(Constants.COMPUTE_GAIN_SIGNAL_BOLT, new ComputeGainSignalBolt())
                .shuffleGrouping(Constants.STOCK_PRICE_TRACKER_SPOUT);

        StormTopology topology = builder.createTopology();

        // Configuration
        Config config = new Config();
        config.setDebug(true);
        config.put("fileToWrite", "D:\\LOGS\\yahoofinance\\output.txt");

        //Submit Topology to cluster
        try {
            LocalCluster cluster = new LocalCluster();

            cluster.submitTopology(Constants.TOPOLOGY_NAME, config, topology);
            Thread.sleep(10000);

            cluster.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
