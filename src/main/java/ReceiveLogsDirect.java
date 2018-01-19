import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeoutException;

/**
 * 接收日志
 *
 * @author 祁军
 */
public class ReceiveLogsDirect {
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
        channel.exchangeDeclare(EXCHANGE_NAME, "direct");
        // 设置日志级别
        int rand = new Random().nextInt(3);

        // 创建三个非持久的、唯一的、自动删除的队列，分别接收不同的日志信息
        String debugQueueName = channel.queueDeclare().getQueue();
        String InfoQueueName = channel.queueDeclare().getQueue();
        String ErrorQueueName = channel.queueDeclare().getQueue();
        // 绑定交换器和队列
        // queueBind(String queue, String exchange, String routingKey)
        // 参数1 queue ：队列名
        // 参数2 exchange ：交换器名
        // 参数3 routingKey ：路由键名
        channel.queueBind(debugQueueName, EXCHANGE_NAME, LOG_LEVEL_ARR[0]);
        channel.queueBind(InfoQueueName, EXCHANGE_NAME, LOG_LEVEL_ARR[1]);
        channel.queueBind(ErrorQueueName, EXCHANGE_NAME, LOG_LEVEL_ARR[2]);

        // rabbit mq 消息的推送支持poll 也支持订阅，先创建一个poll 方式的comsumer
        QueueingConsumer pollConsumer = new QueueingConsumer(channel);
        channel.basicConsume(ErrorQueueName, true, pollConsumer);

        // 创建订阅类型的消费者
        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("Received '" + message + "' from "+envelope.getRoutingKey()+ " by subscribe" );
            }
        };
        channel.basicConsume(debugQueueName, true, consumer);
        channel.basicConsume(InfoQueueName, true, consumer);


        while (true) {
            QueueingConsumer.Delivery delivery = null;
            try {
                delivery = pollConsumer.nextDelivery();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            String message = new String(delivery.getBody());
            String routingKey = delivery.getEnvelope().getRoutingKey();


            System.out.println("Received '" + message + "' from "+routingKey +" by poll");
        }

    }
}
