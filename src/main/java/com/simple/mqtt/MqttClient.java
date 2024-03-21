package com.simple.mqtt;


import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Promise;

import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * mqtt客户端
 *
 * @author gzy
 */
public class MqttClient {

    public static Channel channel;

    protected EventLoopGroup eventLoopGroup;

    //消息id
    private final AtomicInteger nextMessageId = new AtomicInteger(1);
    //未释放的qos2消息id
    private ConcurrentHashMap<Integer, Integer> pendingMessageIdForQos2 = new ConcurrentHashMap<>();
    //服务器未确认的完成消息QOS1
    private ConcurrentHashMap<Integer, MqttPendingMessage> pendingMessageQos1 = new ConcurrentHashMap<>();
    //服务器未确认的完成消息QOS2
    private ConcurrentHashMap<Integer, MqttPendingMessage> pendingMessageQos2 = new ConcurrentHashMap<>();
    //收到消息QOS2
    private ConcurrentHashMap<Integer, MqttPublishMessage> incomingMessageQos2 = new ConcurrentHashMap<>();
    //未完成订阅的主题的许诺
    ConcurrentHashMap<Integer, Promise<Void>> subscribePromises = new ConcurrentHashMap<>();
    //未完成取消订阅的主题的许诺
    ConcurrentHashMap<Integer, Promise<Void>> unSubscribePromises = new ConcurrentHashMap<>();
    //发送消息的滑动队列
    LinkedBlockingQueue<MqttMessage> pendingMessagesQueue = new LinkedBlockingQueue(100);
    //事件回调
    private MqttCallback mqttCallback;

    private MqttConfig mqttConfig;

    public boolean isConnect = false;

    Bootstrap bootstrap = new Bootstrap();

    public MqttClient(MqttConfig mqttConfig) {
        this.mqttConfig = mqttConfig;

    }

    public void setMqttCallbackAndConnect(MqttCallback mqttCallback) throws ExecutionException, InterruptedException {
        this.mqttCallback = mqttCallback;
        final Promise<ConnackRestlt> connect = this.connect();
        connect.get();
    }

    public ConcurrentHashMap<Integer, MqttPublishMessage> getIncomingMessageQos2() {
        return incomingMessageQos2;
    }

    public MqttCallback getMqttCallback() {
        return mqttCallback;
    }

    public ConcurrentHashMap<Integer, Integer> getPendingMessageIdForQos2() {
        return pendingMessageIdForQos2;
    }

    public ConcurrentHashMap<Integer, MqttPendingMessage> getPendingMessageQos1() {
        return pendingMessageQos1;
    }

    public ConcurrentHashMap<Integer, MqttPendingMessage> getPendingMessageQos2() {
        return pendingMessageQos2;
    }

    public MqttConfig getMqttConfig() {
        return mqttConfig;
    }

    public Promise<ConnackRestlt> connect() {
        return connect(-1);
    }

    public Promise<ConnackRestlt> connect(int eventLoop) {
        if (eventLoop < 0) {
            eventLoopGroup = new NioEventLoopGroup();
        } else {
            eventLoopGroup = new NioEventLoopGroup(eventLoop);
        }
        //许诺一个连接结果
        final Promise<ConnackRestlt> futureReslt = new DefaultPromise<ConnackRestlt>(this.eventLoopGroup.next());
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.remoteAddress(this.mqttConfig.getHost(), this.mqttConfig.getPort());
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            protected void initChannel(SocketChannel socketChannel) throws Exception {
                final ChannelPipeline pipeline = socketChannel.pipeline();
                pipeline.addLast("decode", new MqttDecoder());
                pipeline.addLast("encode", MqttEncoder.INSTANCE);
                //设置写空闲心跳
                pipeline.addLast("heartBeat", new IdleStateHandler(0, mqttConfig.getHeartbeatInterval(), 0));
                pipeline.addLast("handler", new MqttInChannelHandler(futureReslt, MqttClient.this));
            }
        });
        //连接
        connect(bootstrap);
        return futureReslt;
    }

    private void connect(Bootstrap bootstrap) {
        ChannelFuture channelFuture = bootstrap.connect();
        channelFuture.addListener(f -> {
                    if (f.isSuccess()) {
                        System.out.println("connect success--");
                    } else {
                        System.out.println("connect fail---");
                        MqttClient.this.isConnect = false;
                    }
                }
        );
        channel = channelFuture.channel();
        channel.closeFuture().addListener(f -> {
            MqttClient.this.isConnect = false;
            this.mqttCallback.connectionLost(new Throwable("MQTT连接丢失"));
            //是否需要重连
            if (mqttConfig.isReconnect()) {
                System.out.println("断线准备重连");
                eventLoopGroup.schedule(() -> connect(bootstrap), 3, TimeUnit.SECONDS);
            } else {
                System.out.println("mqtt连接丢失，不需要重连");
            }
        });
    }

    /**
     * 留给用户手动重连
     */
    public void reconnect() {
        if (getMqttConfig().isReconnect()) {
            return;
        }
        System.out.println("用户手动重连");
        connect(bootstrap);
    }


    public Promise<Void> publish(String topic, String payload, MqttQoS qos) {
        return publish(topic, payload, qos, false);
    }

    public Promise<Void> publish(String topic, String payload, MqttQoS qos, boolean retain) {
        Promise<Void> future = new DefaultPromise<>(eventLoopGroup.next());
        final MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, qos, retain, 0);
        final int nextMessageId = getNextMessageId();
        final MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(topic, nextMessageId);
        final MqttPublishMessage mqttPublishMessage = new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, Unpooled.copiedBuffer(payload, UTF_8));
        final MqttPendingMessage mqttPendingMessage = new MqttPendingMessage(mqttPublishMessage.copy(), qos, MqttMessageType.PUBLISH, mqttPublishVariableHeader.packetId(), future);
        //新增待确定的消息
        if (qos == MqttQoS.AT_LEAST_ONCE) {
            pendingMessageQos1.put(mqttPublishVariableHeader.packetId(), mqttPendingMessage);
        }
        if (qos == MqttQoS.EXACTLY_ONCE) {
            synchronized (this) {
                if (pendingMessageIdForQos2.containsKey(nextMessageId)) {
                    System.out.println("当前messageid未释放：" + nextMessageId);
                    return null;
                }
                pendingMessageIdForQos2.put(nextMessageId, nextMessageId);
            }
            pendingMessageQos2.put(mqttPublishVariableHeader.packetId(), mqttPendingMessage);
        }
        channel.writeAndFlush(mqttPublishMessage);
        //QOS不为0的增加重发机制
        if (qos != MqttQoS.AT_MOST_ONCE) {
            mqttPendingMessage.startPublishRestransmission(eventLoopGroup);
        }else {
            return future.setSuccess(null);
        }
        return future;
    }


    /**
     * 获取下一个messageid
     *
     * @return
     */
    int getNextMessageId() {
        nextMessageId.compareAndSet(65535, 1);
        return nextMessageId.getAndIncrement();
    }


    /**
     * 订阅一个主题
     *
     * @param topic
     * @return
     */
    public Promise<Void> subscribe(String topic) {
        return subscribe(topic, MqttQoS.AT_LEAST_ONCE);
    }

    /**
     * 订阅一个主题
     *
     * @param topic
     * @param qos
     * @return
     */
    public Promise<Void> subscribe(String topic, MqttQoS qos) {
        Promise<Void> future = new DefaultPromise<Void>(eventLoopGroup.next());
        final MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.SUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, 0);
        final MqttMessageIdVariableHeader messageIdVariableHeader = MqttMessageIdVariableHeader.from(getNextMessageId());
        final MqttSubscribeMessage mqttSubscribeMessage = new MqttSubscribeMessage(mqttFixedHeader, messageIdVariableHeader, new MqttSubscribePayload(Collections.singletonList(new MqttTopicSubscription(topic, qos))));
        subscribePromises.put(messageIdVariableHeader.messageId(), future);
        this.channel.writeAndFlush(mqttSubscribeMessage);
        return future;
    }


    /**
     * 取消订阅一个主题
     *
     * @param topic
     * @return
     */
    public Promise<Void> unsubscribe(String topic) {
        Promise<Void> future = new DefaultPromise<Void>(eventLoopGroup.next());
        final MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, 0);
        final MqttMessageIdVariableHeader messageIdVariableHeader = MqttMessageIdVariableHeader.from(getNextMessageId());
        final MqttUnsubscribeMessage mqttUnsubscribeMessage = new MqttUnsubscribeMessage(mqttFixedHeader, messageIdVariableHeader, new MqttUnsubscribePayload(Collections.singletonList(topic)));
        unSubscribePromises.put(messageIdVariableHeader.messageId(), future);
        channel.writeAndFlush(mqttUnsubscribeMessage);
        return future;
    }

}
