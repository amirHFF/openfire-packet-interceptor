package io.projectZ.eventListener.kafka.encoder;
/*
  Project : openfire-interceptor-plugin
  Author  : AmirHFF
  Created : 9/25/2026 - 6:53 PM
*/

import io.projectZ.eventListener.kafka.dto.DirectMessage;
import io.projectZ.eventListener.kafka.dto.KafkaMessage;

public interface MessageByteEncoder {

    byte[] byteEncode(KafkaMessage kafkaMessage);
}
