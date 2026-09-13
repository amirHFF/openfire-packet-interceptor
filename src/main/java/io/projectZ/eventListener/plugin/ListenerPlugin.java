package io.projectZ.eventListener.plugin;
/*
  Project : openfire-interceptor-plugin
  Author  : AmirHFF
  Created : 9/7/2026 - 9:04 PM
*/

import io.projectZ.eventListener.handler.EventHandler;
import io.projectZ.eventListener.handler.InterceptorHandler;
import io.projectZ.eventListener.handler.dto.InterceptorHandlerDTO;
import io.projectZ.eventListener.kafka.KafkaMessageProducer;
import io.projectZ.eventListener.kafka.ProducerQueueThreadPool;
import io.projectZ.eventListener.listener.MessagePacketInterceptor;
import org.jivesoftware.openfire.container.Plugin;
import org.jivesoftware.openfire.container.PluginManager;
import org.jivesoftware.openfire.interceptor.InterceptorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

public class ListenerPlugin implements Plugin {
    private static final Logger logger =
            LoggerFactory.getLogger(ListenerPlugin.class);
    private List<ThreadPoolExecutor> poolExecutors = new ArrayList<>();
    @Override
    public void initializePlugin(PluginManager pluginManager, File file) {
        logger.info("listener plugin initializing .... ");
        KafkaMessageProducer kafkaMessageProducer= new KafkaMessageProducer();
        ThreadPoolExecutor messageInterceptorThreadPool = ProducerQueueThreadPool.create();
        poolExecutors.add(messageInterceptorThreadPool);
        logger.info("thread pool created ...");
        EventHandler<InterceptorHandlerDTO> interceptorHandlerDTOEventHandler = new InterceptorHandler(kafkaMessageProducer,messageInterceptorThreadPool);
        MessagePacketInterceptor packetInterceptor = new MessagePacketInterceptor(interceptorHandlerDTOEventHandler);
        InterceptorManager.getInstance().addInterceptor(packetInterceptor);
        logger.info("listener initializing completed .");
    }

    @Override
    public void destroyPlugin() {
        for (ThreadPoolExecutor executor : poolExecutors) {
            ProducerQueueThreadPool.shutdown(executor);
        }
        logger.info("plugin shutting down gracefully");
    }
}

