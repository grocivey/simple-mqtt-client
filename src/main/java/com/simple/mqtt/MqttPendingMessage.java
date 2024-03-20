package com.simple.mqtt;

import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;

import java.util.concurrent.TimeUnit;

/**
 * 待确定的消息
 */
public class MqttPendingMessage {
    //真实消息
    MqttPublishMessage message;
    //rel消息
    MqttMessage pubrel;
    //qos
    private MqttQoS qos;
    //发送类型
    private MqttMessageType mqttMessageType;
    //消息id
    private int messageId;
    //许诺
    private Promise<Void> future;

    private ScheduledFuture<?> scheduledFuturePublish;
    private ScheduledFuture<?> scheduledFuturePubrel;

    public MqttPendingMessage(MqttPublishMessage message, MqttQoS qos, MqttMessageType mqttMessageType, int messageId, Promise<Void> future) {
        this.message = message;
        this.qos = qos;
        this.mqttMessageType = mqttMessageType;
        this.messageId = messageId;
        this.future = future;
    }


    public MqttQoS getQos() {
        return qos;
    }

    public void setQos(MqttQoS qos) {
        this.qos = qos;
    }

    public MqttMessageType getMqttMessageType() {
        return mqttMessageType;
    }

    public void setMqttMessageType(MqttMessageType mqttMessageType) {
        this.mqttMessageType = mqttMessageType;
    }

    public int getMessageId() {
        return messageId;
    }

    public void setMessageId(int messageId) {
        this.messageId = messageId;
    }


    public Promise<Void> getFuture() {
        return future;
    }

    public void setFuture(Promise<Void> future) {
        this.future = future;
    }

    /**
     * 重发publish机制
     *
     * @param eventLoopGroup
     */
    public void startPublishRestransmission(EventLoopGroup eventLoopGroup) {
        scheduledFuturePublish = eventLoopGroup.scheduleWithFixedDelay(() -> {
            System.out.println("重发publish：" + messageId);
            try {
                MqttClient.channel.writeAndFlush(message.copy());
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("重发失败");
            }
            System.out.println("重发publishend：" + messageId);
        }, 3, 3, TimeUnit.SECONDS);
    }

    /**
     * 重发pubrel机制
     *
     * @param eventLoopGroup
     */
    public void startPubrelRetransmission(EventLoopGroup eventLoopGroup) {
        stopPublishRestransmission();
        scheduledFuturePubrel = eventLoopGroup.scheduleWithFixedDelay(() -> {
            System.out.println("重发pubrel：" + messageId);
            try {
                MqttClient.channel.writeAndFlush(pubrel);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("重发失败");
            }
            System.out.println("重发pubrelend：" + messageId);
        }, 3, 3, TimeUnit.SECONDS);
    }

    public void stopPublishRestransmission() {
        if (scheduledFuturePublish != null) {
            scheduledFuturePublish.cancel(true);
        }
    }

    public void stopPubrelRestransmission() {
        if (scheduledFuturePubrel != null) {
            scheduledFuturePubrel.cancel(true);
        }
    }
}
