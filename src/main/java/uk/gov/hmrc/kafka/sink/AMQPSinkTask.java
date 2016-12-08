package uk.gov.hmrc.kafka.sink;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ReturnListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static uk.gov.hmrc.kafka.sink.AMQPSinkConfig.*;

/**
 * SinkTask that handles sending the actual events to AMQP.
 */
public class AMQPSinkTask extends SinkTask {

  private static final Logger LOG = LoggerFactory.getLogger(AMQPSinkTask.class);

  private ConnectionFactory connectionFactory;

  private Connection connection;

  private Channel channel;

  private String exchangeName;

  private AMQP.BasicProperties basicProperties;

  private int deliveryConfirmationTimeout;

  private int errorRetryTimeout;

  public String version() {
    return "1.0";
  }

  public void start(Map<String, String> configProps) {
    LOG.info("Starting task " + getClass().getSimpleName() + "...");

    if (connectionFactory == null) {
      connectionFactory = new ConnectionFactory();
      connectionFactory.setHost(configProps.get(AMQP_HOST_CONFIG));
      connectionFactory.setPort(Integer.valueOf(configProps.get(AMQP_PORT_CONFIG)));
      connectionFactory.setUsername(configProps.get(AMQP_USERNAME_CONFIG));
      connectionFactory.setPassword(configProps.get(AMQP_PASSWORD_CONFIG));
    }
    LOG.info("AMQP connection host is : {}", configProps.get(AMQP_HOST_CONFIG));
    LOG.info("AMQP connection port is : {}", configProps.get(AMQP_PORT_CONFIG));

    exchangeName = configProps.get(AMQP_EXCHANGE_CONFIG);
    deliveryConfirmationTimeout = Integer.valueOf(configProps.get(AMQP_CONFIRM_TIMEOUT_CONFIG));
    LOG.info("Set AMQP delivery confirmation timeout to : {} ms", deliveryConfirmationTimeout);

    AMQP.BasicProperties.Builder propsBuilder = new AMQP.BasicProperties.Builder();
    basicProperties = propsBuilder
        .contentType("application/json")
        .contentEncoding("UTF-8")
        .build();

    errorRetryTimeout = Integer.valueOf(configProps.get(RETRY_TIMEOUT_CONFIG));
    LOG.info("Set retry timeout to : {} ms", errorRetryTimeout);
  }

  public void put(Collection<SinkRecord> sinkRecords) {

    if (connection == null) {
      try {
        String connectionName = buildConnectionName();
        connection = connectionFactory.newConnection(connectionName);
        LOG.info("Opened new connection to RabbitMQ with name {}", connectionName);

      } catch (Exception e) {
        context.timeout(errorRetryTimeout);
        throw new RetriableException("Error opening a new connection to AMQP", e);
      }
    }

    if (channel == null) {
      try {
        channel = connection.createChannel();
        channel.confirmSelect();
        addChannelListeners();

      } catch (IOException e) {
        context.timeout(errorRetryTimeout);
        closeConnection();
        throw new RetriableException("Error creating a channel on an AMQP connection", e);
      }
    }

    try {
      for (SinkRecord record : sinkRecords) {
        String event = (String) record.value();
        channel.basicPublish(exchangeName, "", basicProperties, event.getBytes());

        LOG.debug("Delivered message");
      }

      channel.waitForConfirmsOrDie(deliveryConfirmationTimeout);
      LOG.debug("Events confirmed");

    } catch (Exception e) {
      context.timeout(errorRetryTimeout);
      closeConnection();
      throw new RetriableException("Error sending events to AMQP", e);
    }
  }

  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
  }

  public void stop() {
    closeConnection();
    LOG.info("Stopped task " + getClass().getSimpleName() + ".");
  }

  public void setConnectionFactory(ConnectionFactory connectionFactory) {
    this.connectionFactory = connectionFactory;
  }

  private void closeConnection() {
    try {
      if (channel != null) {
        channel.close();
        LOG.warn("Closed AMQP channel");
      }

      if (connection != null) {
        connection.close();
        LOG.warn("Closed AMQP connection");
      }

    } catch (Throwable e) {
      // just print the message without being too verbose
      LOG.error("Error closing AMQP connection : {}", e.getMessage());

    } finally {
      channel = null;
      connection = null;
    }
  }

  private void addChannelListeners() {
    channel.addConfirmListener(new ConfirmListener() {
      @Override
      public void handleAck(long deliveryTag, boolean multiple)
          throws IOException {

        LOG.debug("Received ACK for deliveryTag=" + deliveryTag
            + " and multiple=" + multiple);
      }

      @Override
      public void handleNack(long deliveryTag, boolean multiple)
          throws IOException {

        LOG.debug("Received NACK for deliveryTag=" + deliveryTag
            + " and multiple=" + multiple);
      }
    });

    channel.addReturnListener(new ReturnListener() {
      @Override
      public void handleReturn(int replyCode, String replyText, String exchange,
                               String routingKey,
                               AMQP.BasicProperties basicProperties,
                               byte[] bytes) throws IOException {
        LOG.debug("Received return for exchange=" + exchange + " and replyText="
            + replyText);
      }
    });
  }
}
