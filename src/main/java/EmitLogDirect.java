import com.rabbitmq.client.*;
import javafx.scene.media.SubtitleTrack;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 发送日志
 *
 * @author 祁军
 */
public class EmitLogDirect {
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
        // 设定信道的confirm模式
//        channel.confirmSelect();
        channel.txSelect();
//        channel.basicAck();
        // 发送消息

         channel.queueDeclare("queue",true,false,false,null).getQueue();
         channel.queueBind("queue",EXCHANGE_NAME,"");
         long start = System.currentTimeMillis();
         AtomicInteger count = new AtomicInteger(0);
//         channel.

        ExecutorService service = Executors.newFixedThreadPool(2);

       while(System.currentTimeMillis() < start+1000){
//            Runnable runnable = ()-> {
                String message = "Qijun-MSG log : [" + "]" + UUID.randomUUID().toString();
                // 发布消息至交换器，设置mandatory 为true 可以设置 return_call_back
                try {
                    channel.basicPublish(EXCHANGE_NAME, "", true, MessageProperties.MINIMAL_PERSISTENT_BASIC, message.getBytes());
                    channel.txCommit();
                } catch (Exception e) {
                }
                if (System.currentTimeMillis() <= start + 1000) {
                   count.incrementAndGet();
                }

            ;


        }

//        for(int i =0 ;i<10; i++){
//
//            System.out.println("di"+i);
//            try {
//                channel.basicPublish(EXCHANGE_NAME, "", true, MessageProperties.MINIMAL_PERSISTENT_BASIC, "1111".getBytes());
//                channel.txCommit();
//            } catch (Exception e) {
//
//            }
//            System.out.println("di2"+i);
//        }
        System.out.println("count="+ count);



//        channel.addConfirmListener(new ConfirmListener(){
//            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
//                System.out.println("deliveryTag= "+deliveryTag+"multiple= "+multiple);
//
//            }
//
//            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
//                System.out.println("nack"+"deliveryTag= "+deliveryTag+"multiple= "+multiple);
//            }
//        });

        // 关闭频道和连接

//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        service.shutdown();
        channel.close();
        connection.close();
        // ooooooo

        // 22222222

        }
    }