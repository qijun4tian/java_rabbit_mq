import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试confirm callback 和return callback
 *  错误的routingkey 下confirmCallback 没有返回
 *  returnCallback 有返回 但没有消息体
 *  错误的交换机则会报 channel is already closed due to channel error;
 * @author 祁军
 */
public class TestReturnAndConfirm {


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
        channel.exchangeDeclare(EXCHANGE_NAME, "direct",true);


        channel.queueDeclare("queue",true,false,false,null).getQueue();
        channel.queueBind("queue",EXCHANGE_NAME,"");
        long start = System.currentTimeMillis();
        AtomicInteger count = new AtomicInteger(0);



        channel.confirmSelect();
        channel.addConfirmListener(new ConfirmListener(){
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                System.out.println("deliveryTag= "+deliveryTag+"multiple= "+multiple);

            }
            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                System.out.println("nack"+"deliveryTag= "+deliveryTag+"multiple= "+multiple);
            }
        });
        channel.addReturnListener(new ReturnListener() {
            @Override
            public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("return...replyCode="+replyCode+" replyText="+replyText);
            }
        });


        /**测试confirm callback 和return callback**/
        // 测试一个错误的routingkey
        for(int i =0 ;i<10; i++){
            try {
                channel.basicPublish(EXCHANGE_NAME, "", true, MessageProperties.MINIMAL_PERSISTENT_BASIC, "1111".getBytes());
            } catch (Exception e) {

            }
        }
        System.out.println("count="+ count);



        /**测试confirm callback 和return callback**/

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        channel.close();
        connection.close();

    }
}
