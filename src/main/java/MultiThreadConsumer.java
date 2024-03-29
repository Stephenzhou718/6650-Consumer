import com.google.gson.Gson;
import com.rabbitmq.client.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiThreadConsumer {

  private static final String QUEUE_NAME = "skiers-data";
  private static final int NUM_OF_CONSUMERS = 50;
  private final ConnectionFactory factory;

  private Map<Integer, List<String>> skierId2Record = new HashMap<>();

  private MongoDBService mongoDBService = new MongoDBService();

  public MultiThreadConsumer(String host) {
    factory = new ConnectionFactory();
    factory.setHost(host);
    factory.setUsername("admin");
    factory.setPassword("admin");
    factory.setVirtualHost("/");
  }

  public static void main(String[] argv) throws Exception {
    MultiThreadConsumer consumer = new MultiThreadConsumer("54.245.21.195");
    consumer.startConsuming();
  }

  public void startConsuming() throws Exception {
    for (int i = 0; i < NUM_OF_CONSUMERS; i++) {
      new Thread(() -> {
        try {
          consume();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }).start();
    }
  }

  private void consume() throws Exception {
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
    System.out.println("Thread-" + Thread.currentThread().getId() + "waiting for messages. To exit press CTRL+C");

    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      String message = new String(delivery.getBody(), "UTF-8");
      Gson gson = new Gson();
      SkiersLog log = gson.fromJson(message, SkiersLog.class);

      int skierId = log.getSkierID();
      if (!skierId2Record.containsKey(skierId)) {
        skierId2Record.put(skierId, new ArrayList<>());
      }
      skierId2Record.get(skierId).add(message);
      mongoDBService.addSkierLog(log);
      channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
      System.out.println(" [x] Received '" + message + "'");
    };

    channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});
  }
}
