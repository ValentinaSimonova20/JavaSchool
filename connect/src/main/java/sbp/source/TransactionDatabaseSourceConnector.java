package sbp.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TransactionDatabaseSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(TransactionDatabaseSourceConnector.class);
    public static final String TOPIC_CONFIG = "topic";
    public static final String DB_CONFIG = "database";
    public static final String TASK_BATCH_SIZE_CONFIG = "batch.size";

    public static final int DEFAULT_TASK_BATCH_SIZE = 2000;

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(DB_CONFIG, ConfigDef.Type.STRING, "transactions", ConfigDef.Importance.HIGH, "Source database")
            .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH, "The topic to publish data to")
            .define(TASK_BATCH_SIZE_CONFIG, ConfigDef.Type.INT, DEFAULT_TASK_BATCH_SIZE, ConfigDef.Importance.LOW,
                    "The maximum number of records the source task can read from the file each time it is polled");

    private Map<String, String> props;

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        log.info(
                "Starting db source connector reading from {}",
                new AbstractConfig(CONFIG_DEF, props).getString(DB_CONFIG)
        );
    }

    @Override
    public Class<? extends Task> taskClass() {
        return TransactionDatabaseSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        configs.add(props);
        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
