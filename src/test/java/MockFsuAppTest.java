import com.google.common.base.Stopwatch;
import com.simple.mqtt.MqttCallback;
import com.simple.mqtt.MqttClient;
import com.simple.mqtt.MqttConfig;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.concurrent.Promise;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;

public class MockFsuAppTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final AtomicInteger sc = new AtomicInteger(0);
        final AtomicInteger sc2 = new AtomicInteger(0);
        final MqttConfig mqttConfig = new MqttConfig("simple-netty-mqtt-client", "10.180.35.70", 1883, "admin", "public");
        mqttConfig.setCleanSession(false);
        mqttConfig.setReconnect(true);
        MqttClient mqttClient = new MqttClient(mqttConfig);
        mqttClient.setMqttCallbackAndConnect(new MqttCallback() {
            public void sendSuccess(String topic, Integer messageId) {
                System.out.println("发送成功:"+topic+"--"+messageId);
                sc.incrementAndGet();
            }
            public void messageArrived(String topic, MqttMessage message) {
                System.out.println("收到消息"+topic+",message:"+((ByteBuf)message.payload()).toString(UTF_8)+"--"+((MqttPublishVariableHeader)message.variableHeader()).packetId());
            }
            public void connectionLost(Throwable cause) {
                if (cause!=null){
                    cause.printStackTrace();
                }

            }
            public void connetSuccess(boolean reconnet, String serverUrl) {
                System.out.println("mqtt connect success --> " + serverUrl);
            }
        });
        final Promise<Void> xixihaha2 = mqttClient.subscribe("xixihaha2", MqttQoS.EXACTLY_ONCE);
        xixihaha2.get();
        System.out.println("xixihaha2"+" => 订阅成功");
        Stopwatch stopwatch = Stopwatch.createStarted();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(20, 20, 0L, TimeUnit.SECONDS, new LinkedBlockingQueue<>() );
        for (int k = 0; k < 12; k++) {
            executor.execute(()->{
                for (int i = 0; i < 12; i++) {
                    final Promise<Void> publish = mqttClient.publish("test222", "hello通道连接成功 world订阅成功", MqttQoS.EXACTLY_ONCE);
                    sc2.incrementAndGet();
                    try {
                        if (publish == null){
                            System.out.println("发送失败");
                            continue;
                        }
                        publish.get(5, TimeUnit.SECONDS);
                    } catch (InterruptedException | ExecutionException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    } catch (TimeoutException e) {
                        System.out.println("发送超时");
                    }finally {

                    }
                }//7509
            });
        }
        executor.shutdown();
        while (true) {
            if (executor.awaitTermination(1, TimeUnit.SECONDS)) break;
        }
        stopwatch.stop();
        System.out.println(stopwatch);
        System.out.println("发送消息成功:"+sc2);
    }


}
