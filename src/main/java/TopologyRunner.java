import bolt.ComputeGainSignalBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import spout.StockPriceSpout;
import util.Constants;

public class TopologyRunner {
    public static void main(String[] args) throws Exception {

        // Build Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(Constants.STOCK_PRICE_TRACKER_SPOUT, new StockPriceSpout());
        builder.setBolt(Constants.COMPUTE_GAIN_SIGNAL_BOLT, new ComputeGainSignalBolt())
                .shuffleGrouping(Constants.STOCK_PRICE_TRACKER_SPOUT);

        StormTopology topology = builder.createTopology();

        // Configuration
        Config config = new Config();
        config.setDebug(true);

        //Submit Topology to cluster
        if (args != null && args.length > 0) {
//            config.setNumWorkers();
            // submit to remote cluster
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            return;
        }

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(Constants.TOPOLOGY_NAME, config, topology);
    }
}
