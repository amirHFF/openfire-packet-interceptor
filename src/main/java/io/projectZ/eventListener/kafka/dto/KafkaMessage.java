package io.projectZ.eventListener.kafka.dto;
/*
  Project : openfire-interceptor-plugin
  Author  : AmirHFF
  Created : 9/7/2026 - 1:40 PM
*/

import org.xmpp.packet.Message;

import java.io.Serializable;
import java.util.UUID;

public abstract class KafkaMessage implements Serializable {

    private String consumerGroupIdentifier;
    private String id;
    private Message.Type type;
    private long timestamp;
    private byte[] rawPayload;


    public KafkaMessage() {
        id = UUID.randomUUID().toString();
        this.timestamp = System.currentTimeMillis();
    }

    public String getConsumerGroupIdentifier() {
        return consumerGroupIdentifier;
    }

    public void setConsumerGroupIdentifier(String consumerGroupIdentifier) {
        this.consumerGroupIdentifier = consumerGroupIdentifier;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Message.Type getType() {
        return type;
    }

    public void setType(Message.Type type) {
        this.type = type;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }


    public byte[] getRawPayload() {
        return rawPayload;
    }

    public void setRawPayload(byte[] rawPayload) {
        this.rawPayload = rawPayload;
    }
}

