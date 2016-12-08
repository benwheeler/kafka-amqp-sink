package uk.gov.hmrc.kafka.sink;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.WorkerSinkTaskContext;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.hmrc.kafka.sink.AMQPSinkConfig.AMQP_CONFIRM_TIMEOUT_CONFIG;
import static uk.gov.hmrc.kafka.sink.AMQPSinkConfig.AMQP_EXCHANGE_CONFIG;
import static uk.gov.hmrc.kafka.sink.AMQPSinkConfig.RETRY_TIMEOUT_CONFIG;

/**
 * Tests the AMQPSinkTask.
 */
@RunWith(MockitoJUnitRunner.class)
public class AMQPSinkTaskTest {

  private static final String EXCHANGE_NAME = "testExchange";
  private static final Integer CONFIRM_TIMEOUT = 5000;
  private static final Integer RETRY_TIMEOUT = 5000;
  public static final String CONNECTION_NAME = AMQPSinkConfig.buildConnectionName();

  @Mock
  private ConnectionFactory connectionFactory;

  @Mock
  private Connection connection;

  @Mock
  private Channel channel;

  @Mock
  private SinkTaskContext context;

  private AMQPSinkTask sinkTask;

  @Before
  public void setupSink() {
    sinkTask = new AMQPSinkTask();
    sinkTask.setConnectionFactory(connectionFactory);
    sinkTask.initialize(context);

    HashMap<String, String> props = new HashMap<>();
    props.put(AMQP_EXCHANGE_CONFIG, EXCHANGE_NAME);
    props.put(AMQP_CONFIRM_TIMEOUT_CONFIG, CONFIRM_TIMEOUT.toString());
    props.put(RETRY_TIMEOUT_CONFIG, RETRY_TIMEOUT.toString());
    sinkTask.start(props);
  }

  @Test
  public void messagesPublishedInSequence() throws Exception {
    whenCreatingChannel();

    deliverEvents("first event", "second event", "third event");

    inOrder(channel).verify(channel).basicPublish(eq(EXCHANGE_NAME), eq(""),
        any(), eq("first event".getBytes()));
    inOrder(channel).verify(channel).basicPublish(eq(EXCHANGE_NAME), eq(""),
        any(), eq("second event".getBytes()));
    inOrder(channel).verify(channel).basicPublish(eq(EXCHANGE_NAME), eq(""),
        any(), eq("third event".getBytes()));
    inOrder(channel).verify(channel).waitForConfirmsOrDie(CONFIRM_TIMEOUT);

    verify(channel, never()).close();
    verify(connection, never()).close();
  }

  @Test
  public void connectionClosedOnSinkStop() throws Exception {
    whenCreatingChannel();

    deliverEvents("first event");
    sinkTask.stop();

    verifyConnectionClosed();
  }

  @Test(expected = RetriableException.class)
  public void connectionClosedOnDeliveryError() throws Exception {
    whenCreatingChannel();
    doThrow(new IOException()).when(channel).basicPublish(any(), any(), any(), any());

    deliverEvents("first event");

    verify(channel, never()).waitForConfirmsOrDie(CONFIRM_TIMEOUT);
    verify(context).timeout(RETRY_TIMEOUT);
    verifyConnectionClosed();
  }

  @Test(expected = RetriableException.class)
  public void retrySignalOnChannelFailure() throws Exception {
    when(connectionFactory.newConnection(CONNECTION_NAME)).thenReturn(connection);
    when(connection.createChannel()).thenThrow(new IOException());

    deliverEvents("first event");

    verify(context).timeout(RETRY_TIMEOUT);
    verify(channel, never()).close();
    verify(connection).close();
  }

  @Test(expected = RetriableException.class)
  public void retrySignalOnConnectionFailure() throws Exception {
    when(connectionFactory.newConnection(CONNECTION_NAME)).thenThrow(new IOException());

    deliverEvents("first event");

    verify(context).timeout(RETRY_TIMEOUT);
    verify(channel, never()).close();
    verify(connection, never()).close();
  }

  private void deliverEvents(String... eventContents) {
    int index = 1;
    ArrayList<SinkRecord> records = new ArrayList<>();
    for (String content : eventContents) {
      records.add(new SinkRecord("topic", 1, Schema.STRING_SCHEMA, "key",
          Schema.STRING_SCHEMA, content, index));
      index++;
    }

    sinkTask.put(records);
  }

  private void whenCreatingChannel() throws Exception {
    when(connectionFactory.newConnection(CONNECTION_NAME)).thenReturn(connection);
    when(connection.createChannel()).thenReturn(channel);
  }

  private void verifyConnectionClosed() throws Exception {
    verify(channel).close();
    verify(connection).close();
  }
}
