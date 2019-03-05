import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeoutException;

/**
 *测试手动回ack
 * @author 祁军
 */
public class TestManualAck {
    private static final String EXCHANGE_NAME = "direct_logs";
    private static final String[] LOG_LEVEL_ARR = {"debug", "info", "error"};

    public static void main(String[] args) throws IOException, TimeoutException {
        // 创建连接
        ConnectionFactory factory = new ConnectionFactory();
        // 设置 RabbitMQ 的主机名
        factory.setHost("localhost");
        // 创建一个连接
        Connection connection = factory.newConnection();
        // 创建一个通道
        Channel channel = connection.createChannel();
        // 指定一个交换器
//        channel.exchangeDeclare(EXCHANGE_NAME, "direct",true);



//        String queueName = channel.queueDeclare().getQueue();
        // 绑定交换器和队列
        // queueBind(String queue, String exchange, String routingKey)

//        channel.queueBind(queueName, EXCHANGE_NAME, "");

        // 创建订阅类型的消费者
        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, StandardCharsets.UTF_8 );
                System.out.println("deliveryTag "+envelope.getDeliveryTag()+"Received '" + message + "' from " + envelope.getRoutingKey() + " by subscribe");
                channel.basicAck(envelope.getDeliveryTag(),false);
            }
        };
        channel.basicConsume("queue", false, consumer);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        channel.close();
        connection.close();
    }
}
