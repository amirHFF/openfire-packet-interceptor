package io.projectZ.eventListener.kafka.dto;
/*
  Project : openfire-interceptor-plugin
  Author  : AmirHFF
  Created : 9/7/2026 - 4:46 PM
*/

import org.xmpp.packet.Message;

public class DirectMessage extends ConversationalMessage {
    private String messageId;
    private String conversationId;
    private String sender;
    private String receiver;

    public DirectMessage() {
        setType(Message.Type.chat);
        setConsumerGroupIdentifier("direct-message");
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getConversationId() {
        return conversationId;
    }

    public void setConversationId(String conversationId) {
        this.conversationId = conversationId;
    }

    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public String getReceiver() {
        return receiver;
    }

    public void setReceiver(String receiver) {
        this.receiver = receiver;
    }

    public static String getConversationID(String receiver , String sender){
        return receiver.concat("-").concat(sender);
    }

    @Override
    public String toString() {
        return "PeerToPeerMessage{" +
                "id='" + getId() + '\'' +
                ", messageId='" + messageId + '\'' +
                ", conversationId='" + conversationId + '\'' +
                ", sender='" + sender + '\'' +
                ", receiver='" + receiver + '\'' +
                ", timestamp=" + getTimestamp() +
                '}';
    }
}

