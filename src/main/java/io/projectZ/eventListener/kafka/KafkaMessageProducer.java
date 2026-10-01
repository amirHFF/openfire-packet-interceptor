package io.projectZ.eventListener.kafka;
/*
  Project : openfire-interceptor-plugin
  Author  : AmirHFF
  Created : 9/7/2026 - 1:03 PM
*/

import com.fasterxml.jackson.databind.ObjectMapper;
import io.projectZ.eventListener.kafka.dto.DirectMessage;
import io.projectZ.eventListener.kafka.encoder.DirectMessageByteEncoder;
import io.projectZ.eventListener.kafka.encoder.MessageByteEncoder;
import io.projectZ.eventListener.mapper.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.util.Properties;

public class KafkaMessageProducer  {
    private final Logger logger = LogManager.getLogger(KafkaMessageProducer.class);
    private final Properties props = new Properties();
    private final KafkaProducer kafkaProducer;
    private final MessageByteEncoder byteEncoder;

    public KafkaMessageProducer() {
        byteEncoder = new DirectMessageByteEncoder();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "185.204.170.204:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 70);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        kafkaProducer = new KafkaProducer<>(props);
        logger.info("kafka initialized ...");
    }

    public void publish(String topic, DirectMessage message) {

        if (topic != null) {
            try {
                logger.info("published info :" + message.toString());
                logger.info("topic info :" + topic);
                byte[] payload = byteEncoder.byteEncode(message);
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, message.getConversationId(), payload);
                kafkaProducer.send(record, (recordMetadata, exception) -> {
                    if (recordMetadata != null) {
                        logger.info("event timestamp : " + recordMetadata.timestamp() + " offset :" + recordMetadata.offset());
                        return;
                    }
                    if (exception != null) {
                        logger.error("exception : " + exception);
                    }
                });
            } catch (Exception e) {
                logger.error("ignored exception : ", e);
            }
        } else {
            logger.info("ignored event ...");
        }
    }
}

