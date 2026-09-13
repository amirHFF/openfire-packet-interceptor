package io.projectZ.eventListener.handler;
/*
  Project : openfire-interceptor-plugin
  Author  : AmirHFF
  Created : 9/7/2026 - 11:45 AM
*/

import io.projectZ.eventListener.handler.dto.InterceptorHandlerDTO;
import io.projectZ.eventListener.kafka.KafkaMessageProducer;
import io.projectZ.eventListener.kafka.dto.DirectMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xmpp.packet.Message;

import java.util.concurrent.ThreadPoolExecutor;


public class InterceptorHandler implements EventHandler<InterceptorHandlerDTO> {

    private Logger logger = LogManager.getLogger(InterceptorHandler.class);
    private final KafkaMessageProducer producer;
    private ThreadPoolExecutor executor;
    private final String topicName = "message-archive";

    public InterceptorHandler(KafkaMessageProducer producer , ThreadPoolExecutor threadPoolExecutor) {
        this.producer = producer;
        executor = threadPoolExecutor;
    }

    @Override
    public void handle(InterceptorHandlerDTO interceptorHandlerDTO) {

        if (interceptorHandlerDTO.income()) {
            executor.execute(() -> {

                if (interceptorHandlerDTO.message()!=null && interceptorHandlerDTO.message().getFrom() !=null) {
                    logger.info("incoming message handled {}", interceptorHandlerDTO.message().getFrom().getNode() + interceptorHandlerDTO.message().getTo().getNode());
                    DirectMessage message;
                    try {
                        message = createMessage(interceptorHandlerDTO);
                        logger.info("direct message created");
                    } catch (IllegalArgumentException e) {
                        logger.error("error : " + e);
                        return;
                    }
                    if (message != null) {
                        producer.publish(topicName, message);
                    }
                }
                else {
                    logger.error("message is malformed");
                }
            });
        }
    }

    private DirectMessage createMessage(InterceptorHandlerDTO interceptorHandlerDTO) {
        logger.info("create message");
        if (interceptorHandlerDTO == null) {
            logger.error("incoming interceptor dto is null");
            throw new IllegalArgumentException("incoming interceptor dto is null");
        }

        Message message = interceptorHandlerDTO.message();
        if (message == null) {
            logger.error("incoming message is null");
            throw new IllegalArgumentException("incoming message is null");
        }
        logger.info("create direct message from : "+interceptorHandlerDTO.message().getFrom().toBareJID());

        DirectMessage p2pMessage = new DirectMessage();
        p2pMessage.setMessageId(message.getID());
        p2pMessage.setBody(message.getBody());
        p2pMessage.setSender(message.getFrom().getNode());
        p2pMessage.setReceiver(message.getTo().getNode());
        p2pMessage.setConversationId(DirectMessage.getConversationID(p2pMessage.getReceiver(), p2pMessage.getSender()));

        return p2pMessage;
    }

}

