package com.simple.mqtt;

import io.netty.handler.codec.mqtt.MqttConnectReturnCode;

public class ConnackRestlt {
    MqttConnectReturnCode connectReturnCode;
    boolean isSuccess = false;

    public ConnackRestlt(MqttConnectReturnCode connectReturnCode, boolean isSuccess) {
        this.connectReturnCode = connectReturnCode;
        this.isSuccess = isSuccess;
    }

    public MqttConnectReturnCode getConnectReturnCode() {
        return connectReturnCode;
    }

    public void setConnectReturnCode(MqttConnectReturnCode connectReturnCode) {
        this.connectReturnCode = connectReturnCode;
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public void setSuccess(boolean success) {
        isSuccess = success;
    }
}
