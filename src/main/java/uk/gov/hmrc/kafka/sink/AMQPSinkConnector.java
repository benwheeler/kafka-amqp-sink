package uk.gov.hmrc.kafka.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SinkConnector implementation for AMQP downstream delivery.
 */
public class AMQPSinkConnector extends SinkConnector {

  private static final Logger LOG = LoggerFactory.getLogger(AMQPSinkConnector.class);

  private Map<String, String> configProperties;

  public String version() {
    return "1.0";
  }

  public void start(Map<String, String> props) {
    LOG.info("Starting connector " + getClass().getSimpleName() + "...");
    configProperties = props;
  }

  public Class<? extends Task> taskClass() {
    return AMQPSinkTask.class;
  }

  public List<Map<String, String>> taskConfigs(int maxTasks) {
    ArrayList<Map<String, String>> configs = new ArrayList<>(maxTasks);
    ConfigDef def = config();

    while (configs.size() < maxTasks) {
      HashMap<String, String> config = new HashMap<>();

      for (String key : def.names()) {
        String value = configProperties.get(key);
        config.put(key, value);
      }

      configs.add(config);
    }

    return configs;
  }

  public void stop() {
    LOG.info("Stopped connector " + getClass().getSimpleName() + ".");
  }

  public ConfigDef config() {
    return AMQPSinkConfig.config();
  }
}
