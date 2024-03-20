package com.simple.mqtt;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Promise;

public class MqttInChannelHandler extends SimpleChannelInboundHandler<MqttMessage> {
    private Promise<ConnackRestlt> future;
    private MqttConfig mqttConfig;
    private MqttClient mqttClient;
    private MqttCallback mqttCallback;

    public MqttInChannelHandler(Promise<ConnackRestlt> future, MqttClient mqttClient) {
        this.future = future;
        this.mqttConfig = mqttClient.getMqttConfig();
        this.mqttCallback = mqttClient.getMqttCallback();
        this.mqttClient = mqttClient;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            //构建ping包
            MqttMessage pingMessage = MqttMessageFactory.newMessage(
                    new MqttFixedHeader(MqttMessageType.PINGREQ, false, MqttQoS.AT_MOST_ONCE, false, 0),
                    null,
                    null);
//            System.out.println("发送一次心跳");
            ctx.writeAndFlush(pingMessage);
        } else {
            super.userEventTriggered(ctx, evt);
        }

    }

    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage mqttMessage) throws Exception {
        final MqttMessageType mqttMessageType = mqttMessage.fixedHeader().messageType();
        switch (mqttMessageType) {
            case CONNACK:
                dealConnack(ctx, (MqttConnAckMessage) mqttMessage);
                break;
            case DISCONNECT:
                System.out.println("服务端触发DISCONNECT事件，关闭通道channel");
                this.mqttCallback.connectionLost(null);
                ctx.close();
                break;
            case PUBLISH:
                dealPublish(ctx, (MqttPublishMessage) mqttMessage);
                break;
            case PUBACK:
                dealPuback(ctx, (MqttPubAckMessage) mqttMessage);
                break;
            case PUBREC:
                dealPubrec(ctx, mqttMessage);
                break;
            case PUBREL:
                dealPubrel(ctx, mqttMessage);
                break;
            case PUBCOMP:
                dealPubcomp(ctx, mqttMessage);
                break;
            case SUBACK:
                dealSuback(ctx, (MqttSubAckMessage) mqttMessage);
                break;
            case UNSUBACK:
                dealUnsuback(ctx, (MqttUnsubAckMessage) mqttMessage);
                break;
            case PINGRESP:
                dealPingrsp(ctx, mqttMessage);
                break;
            default:
                ctx.fireChannelRead(mqttMessage);
                ReferenceCountUtil.release(mqttMessage.payload());
        }
    }

    /**
     * 处理服务端发送来的Unsuback
     *
     * @param ctx
     * @param mqttMessage
     */
    private void dealUnsuback(ChannelHandlerContext ctx, MqttUnsubAckMessage mqttMessage) {
        this.mqttClient.unSubscribePromises.get(mqttMessage.variableHeader().messageId()).setSuccess(null);
        this.mqttClient.unSubscribePromises.remove(mqttMessage.variableHeader().messageId());
    }

    /**
     * 处理服务端发送来的Suback
     *
     * @param ctx
     * @param mqttMessage
     */
    private void dealSuback(ChannelHandlerContext ctx, MqttSubAckMessage mqttMessage) {
        this.mqttClient.subscribePromises.get(mqttMessage.variableHeader().messageId()).setSuccess(null);
        this.mqttClient.subscribePromises.remove(mqttMessage.variableHeader().messageId());
    }

    /**
     * 处理Pubrel
     *
     * @param ctx
     * @param mqttMessage
     */
    private void dealPubrel(ChannelHandlerContext ctx, MqttMessage mqttMessage) {
        final int messageId = ((MqttMessageIdVariableHeader) mqttMessage.variableHeader()).messageId();
        final MqttPublishMessage mqttPublishMessage = mqttClient.getIncomingMessageQos2().get(messageId);
        dealBrokerMessage(ctx, mqttPublishMessage);
        //移除messageId的income消息
        mqttClient.getIncomingMessageQos2().remove(messageId);
        //回复comp
        final MqttMessage pubcomp = MqttMessageFactory.newMessage(new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(messageId), null);
        ctx.writeAndFlush(pubcomp);
        ReferenceCountUtil.release(mqttMessage);
    }

    /**
     * 处理服务端发送来的消息
     *
     * @param ctx
     * @param mqttMessage
     */
    private void dealPublish(ChannelHandlerContext ctx, MqttPublishMessage mqttMessage) {
        final MqttQoS mqttQoS = mqttMessage.fixedHeader().qosLevel();
        switch (mqttQoS) {
            case AT_MOST_ONCE:
                dealBrokerMessage(ctx, mqttMessage);
                break;
            case AT_LEAST_ONCE:
                dealBrokerMessage(ctx, mqttMessage);
                //构建puback包
                final MqttMessage puback = MqttMessageFactory.newMessage(new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                        MqttMessageIdVariableHeader.from(mqttMessage.variableHeader().packetId()), null);
                ctx.writeAndFlush(puback);
                break;
            case EXACTLY_ONCE:
                //store
                mqttClient.getIncomingMessageQos2().put(mqttMessage.variableHeader().packetId(), mqttMessage.copy());
                //构建pubrec包
                final MqttMessage pubrec = MqttMessageFactory.newMessage(new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 0),
                        MqttMessageIdVariableHeader.from(mqttMessage.variableHeader().packetId()), null);
                ctx.writeAndFlush(pubrec);
                break;
            default:
                System.out.println("未知的qos");
                ReferenceCountUtil.release(mqttMessage);
        }
    }

    private void dealBrokerMessage(ChannelHandlerContext ctx, MqttPublishMessage mqttMessage) {
        final String topicName = mqttMessage.variableHeader().topicName();
        this.mqttCallback.messageArrived(topicName, mqttMessage);
    }

    private void dealPubcomp(ChannelHandlerContext ctx, MqttMessage mqttMessage) {
        final int messageId = ((MqttMessageIdVariableHeader) mqttMessage.variableHeader()).messageId();
        //许诺发送结果
        mqttClient.getPendingMessageQos2().get(messageId).getFuture().setSuccess(null);
        System.out.println("释放messageId:" + messageId);
        //释放messageId
        mqttClient.getPendingMessageIdForQos2().remove(messageId);
        //删除pulish消息
        final MqttPendingMessage remove = mqttClient.getPendingMessageQos2().remove(messageId);
        //停止重发
        remove.stopPubrelRestransmission();
        //回调
        this.mqttCallback.sendSuccess(remove.message.variableHeader().topicName(), messageId);
    }

    private void dealPubrec(ChannelHandlerContext ctx, MqttMessage mqttMessage) {
        final int messageId = ((MqttMessageIdVariableHeader) mqttMessage.variableHeader()).messageId();
        //构建pubrel
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0);
        final MqttMessage pubrel = MqttMessageFactory.newMessage(fixedHeader, MqttMessageIdVariableHeader.from(messageId), null);
        //回复pubrel
        ctx.writeAndFlush(pubrel);
        //增加重发机制
        MqttPendingMessage pendingMessage = mqttClient.getPendingMessageQos2().get(messageId);
        pendingMessage.pubrel = pubrel;
        pendingMessage.startPubrelRetransmission(this.mqttClient.eventLoopGroup);
        System.out.println("回复pubrel:" + messageId);
    }

    /**
     * 收到到puback包
     *
     * @param ctx
     * @param mqttMessage
     */
    private void dealPuback(ChannelHandlerContext ctx, MqttPubAckMessage mqttMessage) {
        final int messageId = mqttMessage.variableHeader().messageId();
//        System.out.println("收到puback,消息id:" + messageId);
        //许诺发送结果
        final MqttPendingMessage pendingMessage = mqttClient.getPendingMessageQos1().get(messageId);
        if (pendingMessage == null){
            System.out.println("重复ack:"+messageId);
            return;
        }
        pendingMessage.getFuture().setSuccess(null);
        //删除pendingmessage
        final MqttPendingMessage remove = mqttClient.getPendingMessageQos1().remove(messageId);
        remove.stopPublishRestransmission();
        //回调
        this.mqttCallback.sendSuccess(((MqttPublishMessage) remove.message).variableHeader().topicName(), messageId);
//        System.out.println("删除pendingmessageQos1" + remove);

    }

    /**
     * 接受到pingrsp包
     *
     * @param ctx
     * @param mqttMessage
     */
    private void dealPingrsp(ChannelHandlerContext ctx, MqttMessage mqttMessage) {
//        System.out.println("接受到pingrsp包");
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        //通道连接成功
        System.out.println("channelActive: 通道连接成功");
        //发送connect报文
        //构建连接报文
        final MqttConnectMessage connectMessage = MqttMessageBuilders.connect()
                .clientId(this.mqttConfig.getClientId())
                .cleanSession(this.mqttConfig.isCleanSession())
                .keepAlive(this.mqttConfig.getKeepAliveTime())
                .build();
        ctx.writeAndFlush(connectMessage);
    }


    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("channelInactive: 通道连接断开");
        this.mqttCallback.connectionLost(null);
    }

    /**
     * 处理connack报文
     *
     * @param mqttMessage
     */
    private void dealConnack(ChannelHandlerContext ctx, MqttConnAckMessage mqttMessage) {
        final MqttConnectReturnCode mqttConnectReturnCode = mqttMessage.variableHeader().connectReturnCode();
        switch (mqttConnectReturnCode) {
            case CONNECTION_ACCEPTED:
                //是重连就不在设置成功
                if (!future.isSuccess()) {
                    this.future.setSuccess(new ConnackRestlt(mqttConnectReturnCode, true));
                }
                this.mqttClient.isConnect = true;
                this.mqttCallback.connetSuccess(false, this.mqttConfig.getHost() + ":" + this.mqttConfig.getPort());
                break;
            default:
                this.mqttClient.isConnect = false;
                System.out.println("未知连接异常。connack码：" + mqttConnectReturnCode);
                //关闭通道
                ctx.close();

        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        System.out.println("捕获异常");
        cause.printStackTrace();
    }
}
