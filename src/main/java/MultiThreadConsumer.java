import com.rabbitmq.client.*;

public class MultiThreadConsumer {

  private static final String QUEUE_NAME = "skier-data";
  private static final int NUM_OF_CONSUMERS = 5;
  private final ConnectionFactory factory;

  public MultiThreadConsumer(String host) {
    factory = new ConnectionFactory();
    factory.setHost(host);
    factory.setUsername("admin");
    factory.setPassword("admin");
    factory.setVirtualHost("/");
  }

  public static void main(String[] argv) throws Exception {
    MultiThreadConsumer consumer = new MultiThreadConsumer("34.208.141.48");
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
      System.out.println(" [x] Received '" + message + "'");
    };

    channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {});
  }
}
