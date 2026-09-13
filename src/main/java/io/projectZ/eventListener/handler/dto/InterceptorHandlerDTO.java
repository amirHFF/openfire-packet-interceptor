package io.projectZ.eventListener.handler.dto;
/*
  Project : openfire-interceptor-plugin
  Author  : AmirHFF
  Created : 9/7/2026 - 11:48 AM
*/
import org.jivesoftware.openfire.session.Session;
import org.xmpp.packet.Message;

public record InterceptorHandlerDTO(Message message , Session session , boolean income) {

}

