package uk.gov.hmrc.kafka.sink;

import org.apache.kafka.common.config.ConfigDef;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

/**
 * Handles the configuration of the AMQP sink connector.
 */
public class AMQPSinkConfig {

  public static final String AMQP_HOST_CONFIG = "amqpHostname";
  public static final String AMQP_PORT_CONFIG = "amqpPort";
  public static final String AMQP_USERNAME_CONFIG = "amqpUsername";
  public static final String AMQP_PASSWORD_CONFIG = "amqpPassword";
  public static final String AMQP_EXCHANGE_CONFIG = "amqpExchange";
  public static final String AMQP_CONFIRM_TIMEOUT_CONFIG = "amqpConfirmTimeout";

  public static final String RETRY_TIMEOUT_CONFIG = "retryTimeout";

  public static final Integer DEFAULT_CONFIRM_TIMEOUT = 5000;
  public static final Integer DEFAULT_RETRY_TIMEOUT = 5000;

  /**
   * Used to tag AMQP connections with the name of the event source.
   */
  private static final String CONNECTION_PREFIX = "KAFKA_";

  public static ConfigDef config() {
    ConfigDef def = new ConfigDef();

    def.define(AMQP_HOST_CONFIG, STRING, HIGH, "AMQP hostname");
    def.define(AMQP_PORT_CONFIG, INT, HIGH, "AMQP port");
    def.define(AMQP_USERNAME_CONFIG, STRING, HIGH, "AMQP username");
    def.define(AMQP_PASSWORD_CONFIG, STRING, HIGH, "AMQP password");
    def.define(AMQP_EXCHANGE_CONFIG, STRING, HIGH, "AMQP exchange");
    def.define(AMQP_CONFIRM_TIMEOUT_CONFIG, INT, DEFAULT_CONFIRM_TIMEOUT, HIGH,
        "Milliseconds to wait for delivery confirmation before assuming failure");
    def.define(RETRY_TIMEOUT_CONFIG, INT, DEFAULT_RETRY_TIMEOUT, HIGH,
        "Milliseconds to wait after failed event transmission before retrying.");

    return def;
  }

  public static String buildConnectionName() {
    String hostname = "unknown";

    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      // do nothing as we fallback to the default hostname
    }

    return CONNECTION_PREFIX + hostname.toUpperCase();
  }
}
