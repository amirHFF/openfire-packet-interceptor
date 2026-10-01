package io.projectZ.eventListener.kafka.dto;
/*
  Project : openfire-interceptor-plugin
  Author  : AmirHFF
  Created : 9/25/2026 - 7:03 PM
*/

public class ConversationalMessage extends KafkaMessage{
    private String conversationId;

    public String getConversationId() {
        return conversationId;
    }

    public void setConversationId(String conversationId) {
        this.conversationId = conversationId;
    }
}

