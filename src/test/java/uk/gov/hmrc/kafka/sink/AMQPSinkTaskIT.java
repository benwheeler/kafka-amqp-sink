package uk.gov.hmrc.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import info.batey.kafka.unit.KafkaUnit;
import io.arivera.oss.embedded.rabbitmq.EmbeddedRabbitMq;
import io.arivera.oss.embedded.rabbitmq.EmbeddedRabbitMqConfig;
import io.arivera.oss.embedded.rabbitmq.PredefinedVersion;
import io.arivera.oss.embedded.rabbitmq.bin.RabbitMqPlugins;
import io.arivera.oss.embedded.rabbitmq.helpers.ShutDownException;
import kafka.producer.KeyedMessage;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.FutureCallback;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static uk.gov.hmrc.kafka.sink.AMQPSinkConfig.*;

/**
 * Test the behaviour of the AMQPSinkTask.
 */
public class AMQPSinkTaskIT {

  private static final Logger LOG = LoggerFactory.getLogger(AMQPSinkTaskIT.class);

  private static final String MANAGEMENT_PORT = "15672";
  private static final String BROKER_PORT = "5672";
  private static final String BROKER_HOSTNAME = "127.0.0.1";
  private static final String EXCHANGE_NAME = "testExchange";
  private static final String QUEUE_NAME = "testQueue";
  private static final String TEST_TOPIC_NAME = "testTopic";
  private static final String USER_NAME = "guest";
  private static final String PASSWORD = "guest";
  private static final String VIRTUAL_HOST = "/";
  private static final String RABBIT_AUTH = Base64.getEncoder().encodeToString(
      (USER_NAME + ":" + PASSWORD).getBytes());

  private EmbeddedRabbitMq broker;

  private Connection consumerConnection;

  private Connect connect;

  private Channel consumerChannel;

  private CountDownLatch deliveryLatch;

  private KafkaUnit kafkaUnitServer;

  private URI kafkaConnectUri;

  @Test
  public void successfullySendCollectionOfRecords() throws Exception {
    startRabbitMQ();
    startKafka();

    deliveryLatch = new CountDownLatch(3);
    registerEventConsumer();

    kafkaUnitServer.sendMessages(
        new KeyedMessage<>(TEST_TOPIC_NAME, "key", "first event"),
        new KeyedMessage<>(TEST_TOPIC_NAME, "key", "second event"),
        new KeyedMessage<>(TEST_TOPIC_NAME, "key", "third event")
    );

    deliveryLatch.await(5000, TimeUnit.MILLISECONDS);
    assertEquals(0, deliveryLatch.getCount());
  }

  @Test
  public void reconnectToRabbitMQIfConnectionClosed() throws Exception {
    startRabbitMQ();
    startKafka();

    deliveryLatch = new CountDownLatch(2);
    registerEventConsumer();

    kafkaUnitServer.sendMessages(
        new KeyedMessage<>(TEST_TOPIC_NAME, "key", "first event")
    );

    // wait for last message to be delivered
    while (deliveryLatch.getCount() > 2) {
      deliveryLatch.await(500, TimeUnit.MILLISECONDS);
    }

    closeKafkaRabbitMQConnection();

    kafkaUnitServer.sendMessages(
        new KeyedMessage<>(TEST_TOPIC_NAME, "key", "second event")
    );

    deliveryLatch.await(10000, TimeUnit.MILLISECONDS);
    assertEquals(0, deliveryLatch.getCount());
  }

  @Test
  public void connectToRabbitMQWhenItStarts() throws Exception {
    startKafka();

    deliveryLatch = new CountDownLatch(1);

    kafkaUnitServer.sendMessages(
        new KeyedMessage<>(TEST_TOPIC_NAME, "key", "first event")
    );

    startRabbitMQ();
    registerEventConsumer();

    deliveryLatch.await(10000, TimeUnit.MILLISECONDS);
    assertEquals(0, deliveryLatch.getCount());
  }

  private void startKafka() throws Exception {
    kafkaUnitServer = new KafkaUnit();
    kafkaUnitServer.startup();
    kafkaUnitServer.createTopic(TEST_TOPIC_NAME);

    LOG.info("Zookeeper port : {}", kafkaUnitServer.getZkPort());
    LOG.info("Kafka Broker port : {}", kafkaUnitServer.getBrokerPort());
    LOG.info("Kafka Connect : {}", kafkaUnitServer.getKafkaConnect());

    startConnector();
  }

  private void startRabbitMQ() throws Exception {
    EmbeddedRabbitMqConfig config = new EmbeddedRabbitMqConfig.Builder()
        .version(PredefinedVersion.LATEST)
        .rabbitMqServerInitializationTimeoutInMillis(20000)
        .build();
    broker = new EmbeddedRabbitMq(config);
    broker.start();

    RabbitMqPlugins rabbitMqPlugins = new RabbitMqPlugins(config);
    rabbitMqPlugins.enable("rabbitmq_management");
  }

  @After
  public void stopKafka() throws Exception {
    try {
      stopConnector();

    } finally {
      LOG.info("Shutting down Kafka");
      kafkaUnitServer.shutdown();
    }

    LOG.info("Completed Kafka shutdown");
  }

  @After
  public void stopRabbitMQ() throws Exception {
    try {
      LOG.info("Shutting down RabbitMQ client connection");
      consumerChannel.close();
      consumerConnection.close();

    } finally {
      LOG.info("Shutting down RabbitMQ broker");
      try {
        broker.stop();
      } catch (ShutDownException e) {
        // ignore this
      }
    }

    LOG.info("Completed RabbitMQ shutdown");
  }

  private void registerEventConsumer() throws Exception {
    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setHost(BROKER_HOSTNAME);
    connectionFactory.setPort(Integer.valueOf(BROKER_PORT));
    connectionFactory.setUsername(USER_NAME);
    connectionFactory.setPassword(PASSWORD);
    connectionFactory.setVirtualHost(VIRTUAL_HOST);

    consumerConnection = connectionFactory.newConnection(AMQPSinkTaskIT.class.getSimpleName());
    consumerChannel = consumerConnection.createChannel();

    consumerChannel.exchangeDeclare(EXCHANGE_NAME, "direct");
    consumerChannel.queueDeclare(QUEUE_NAME, false, false, false, null);
    consumerChannel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "");

    consumerChannel.basicConsume(QUEUE_NAME, true, getClass().getSimpleName(),
        new DefaultConsumer(consumerChannel) {
          @Override
          public void handleDelivery(String consumerTag, Envelope envelope,
                                     AMQP.BasicProperties properties,
                                     byte[] body) throws IOException {

            deliveryLatch.countDown();
            super.handleDelivery(consumerTag, envelope, properties, body);

            LOG.info("Received event : " + new String(body));
          }
        }
    );
  }

  private void startConnector() {
    String converterClass = StringConverter.class.getCanonicalName();

    Map<String, String> workerProps = new HashMap<>();
    workerProps.put("bootstrap.servers", kafkaUnitServer.getKafkaConnect());
    workerProps.put("key.converter", converterClass);
    workerProps.put("value.converter", converterClass);
    workerProps.put("internal.key.converter", converterClass);
    workerProps.put("internal.value.converter", converterClass);
    workerProps.put("offset.storage.file.filename", "/tmp/blah");

    Time time = new SystemTime();
    StandaloneConfig config = new StandaloneConfig(workerProps);

    RestServer rest = new RestServer(config);
    kafkaConnectUri = rest.advertisedUrl();

    new Thread(() -> {
      String workerId = kafkaConnectUri.getHost() + ":" + kafkaConnectUri.getPort();

      Worker worker = new Worker(workerId, time, config, new MemoryOffsetBackingStore());

      Herder herder = new StandaloneHerder(worker);
      connect = new Connect(herder, rest);

      Map<String, String> connectorProps = new HashMap<>();
      connectorProps.put("name", AMQPSinkConnector.class.getSimpleName());
      connectorProps.put("connector.class", AMQPSinkConnector.class.getCanonicalName());
      connectorProps.put("tasks.max", "1");
      connectorProps.put("key.converter", converterClass);
      connectorProps.put("value.converter", converterClass);
      connectorProps.put("topics", TEST_TOPIC_NAME);
      connectorProps.put(AMQP_HOST_CONFIG, BROKER_HOSTNAME);
      connectorProps.put(AMQP_PORT_CONFIG, BROKER_PORT);
      connectorProps.put(AMQP_USERNAME_CONFIG, USER_NAME);
      connectorProps.put(AMQP_PASSWORD_CONFIG, PASSWORD);
      connectorProps.put(AMQP_EXCHANGE_CONFIG, EXCHANGE_NAME);
      connectorProps.put(AMQP_CONFIRM_TIMEOUT_CONFIG, "5000");
      connectorProps.put(RETRY_TIMEOUT_CONFIG, "1000");

      try {
        connect.start();

        FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>(
            new Callback<Herder.Created<ConnectorInfo>>() {
              @Override
              public void onCompletion(Throwable error, Herder.Created<ConnectorInfo> info) {
                if (error != null)
                  LOG.error("Failed to create job");
                else
                  LOG.info("Created connector {}", info.result().name());
              }
            });

        herder.putConnectorConfig(
            connectorProps.get(ConnectorConfig.NAME_CONFIG),
            connectorProps, false, cb);

        cb.get();

      } catch (Throwable t) {
        LOG.error("Stopping after connector error", t);
        connect.stop();
      }

      connect.awaitStop();
      LOG.info("Stopped connector.");
    }).start();
  }

  private void stopConnector() throws Exception {
    LOG.info("Stopping Kafka connector");

    URI deleteConnectorUri = kafkaConnectUri.resolve(
        "/connectors/" + AMQPSinkConnector.class.getSimpleName());

    callApi(deleteConnectorUri.toURL(), "DELETE", 204, Collections.emptyMap());

    connect.stop();
    connect.awaitStop();
  }

  private void closeKafkaRabbitMQConnection() throws Exception {
    URL listConnUrl = new URL("http://127.0.0.1:" + MANAGEMENT_PORT
        + "/api/connections");

    Map<String, String> headers = new HashMap<>();
    headers.put("Authorization", "Basic " + RABBIT_AUTH);

    String connections = callApi(listConnUrl, "GET", 200, headers);

    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree(connections);
    Iterator<JsonNode> connectionIterator = jsonNode.elements();
    while (connectionIterator.hasNext()) {
      JsonNode connection = connectionIterator.next();
      String clientConnectionName = connection
          .get("client_properties")
          .get("connection_name")
          .asText();

      if (AMQPSinkConfig.buildConnectionName().equals(clientConnectionName)) {
        String rabbitConnectionName = connection.get("name").asText();

        URL closeConnUrl = new URL("http://127.0.0.1:" + MANAGEMENT_PORT
            + "/api/connections/" + URLEncoder.encode(rabbitConnectionName, "UTF-8"));

        callApi(closeConnUrl, "DELETE", 204, headers);
      }
    }
  }

  private String callApi(URL url, String method, int expectedResponse,
                       Map<String, String> headers) throws Exception {

    LOG.info("Calling method {} on API endpoint {}", method, url.toString());

    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod(method);
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      connection.setRequestProperty(entry.getKey(), entry.getValue());
    }
    connection.setDoInput(true);
    connection.connect();

    int httpStatusCode = connection.getResponseCode();
    LOG.info("Got response {} from API {}", httpStatusCode, url.toString());

    String response = IOUtils.toString(connection.getInputStream());
    LOG.info("Response body was : {}", response);

    assertEquals(expectedResponse, httpStatusCode);
    return response;
  }
}
