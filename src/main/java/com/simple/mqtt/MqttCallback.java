package com.simple.mqtt;

import io.netty.handler.codec.mqtt.MqttMessage;

/**
 * 事件消息回调
 *
 * @author gzy
 */
public interface MqttCallback {
    /**
     * 发送成功
     *
     * @param topic
     * @param messageId
     */
    void sendSuccess(String topic, Integer messageId);

    /**
     * 订阅消息到达
     *
     * @param topic
     * @param message
     */
    void messageArrived(String topic, MqttMessage message);

    /**
     * 连接丢失
     *
     * @param cause
     */
    void connectionLost(Throwable cause);

    /**
     * 连接成功
     *
     * @param reconnet
     * @param serverUrl
     */
    void connetSuccess(boolean reconnet, String serverUrl);

}
