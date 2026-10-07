package io.projectZ.eventListener.listener;
/*
  Project : openfire-interceptor-plugin
  Author  : AmirHFF
  Created : 9/7/2026 - 2:21 AM
*/

import io.projectZ.eventListener.handler.EventHandler;
import io.projectZ.eventListener.handler.Hashing;
import io.projectZ.eventListener.handler.dto.InterceptorHandlerDTO;
import org.dom4j.Element;
import org.jivesoftware.openfire.interceptor.PacketInterceptor;
import org.jivesoftware.openfire.session.Session;
import org.xmpp.packet.Packet;
import org.xmpp.packet.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public final class MessagePacketInterceptor implements PacketInterceptor {

    private static final Logger logger =
            LoggerFactory.getLogger(MessagePacketInterceptor.class);

    private final EventHandler<InterceptorHandlerDTO> eventHandler;

    public MessagePacketInterceptor(EventHandler<InterceptorHandlerDTO> eventHandler) {
        this.eventHandler = eventHandler;
    }

    @Override
    public void interceptPacket(Packet packet, Session session, boolean incoming, boolean processed) {
        /*
         * We only care about XMPP Message packets.
         */
        if (!(packet instanceof Message message)) {
            return;
        }

        if (processed) {
            return;
        }

        if (session == null) {
            logger.error("session is null");
            return;
        }
        if (message.getID() == null) {
            message.setID(UUID.randomUUID().toString());
        }

        addConversationHash(message);
        try {
            logger.info("message event received ", message.getID());
            eventHandler.handle(new InterceptorHandlerDTO(message, session, incoming));
        } catch (Exception e) {

            /*
             * NEVER allow an exception in our plugin to break
             * Openfire's packet processing pipeline.
             */
            logger.error(
                    "Failed to process XMPP message packet. " +
                            "packetId={}, from={}, to={}, incoming={}",
                    message.getID(),
                    message.getFrom(),
                    message.getTo(),
                    incoming,
                    e
            );
        }
    }

    private void addConversationHash(Message message) {

        Element zchat = message.getChildElement("zchat", "urn:zchat:message");

        if (zchat != null && zchat.element("hash") != null) {
            return;
        }

        String conversationHash = Hashing.hash(message.getFrom().getNode(), message.getTo().getNode()
        );

        if (zchat == null) {
            zchat = message.addChildElement(
                    "zchat",
                    "urn:zchat:message"
            );
        }

        zchat.addElement("hash")
                .setText(conversationHash);
    }
}

