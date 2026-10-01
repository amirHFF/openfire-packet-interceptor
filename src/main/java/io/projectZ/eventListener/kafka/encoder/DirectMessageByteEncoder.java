package io.projectZ.eventListener.kafka.encoder;
/*
  Project : openfire-interceptor-plugin
  Author  : AmirHFF
  Created : 9/25/2026 - 7:09 PM
*/

import io.projectZ.eventListener.kafka.dto.DirectMessage;
import io.projectZ.eventListener.kafka.dto.KafkaMessage;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DirectMessageByteEncoder implements MessageByteEncoder {
    private static final Schema SCHEMA = SchemaBuilder.record("ChatMessage")
            .namespace("io.projectZ.avroSchema")
            .fields()
            .requiredString("id")
            .requiredString("type")
            .requiredString("consumerGroupIdentifier")
            .requiredString("messageId")
            .requiredString("conversationId")
            .requiredString("senderId")
            .requiredString("receiverId")
            .requiredLong("timestamp")
            .name("rawPayload").type().bytesType().noDefault()
            .endRecord();

    private final DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(SCHEMA);

    @Override
    public byte[] byteEncode(KafkaMessage kafkaMessage) {
        if (kafkaMessage instanceof DirectMessage message) {

            GenericRecord record = new GenericData.Record(SCHEMA);
            record.put("id", message.getId());
            if (message.getType() != null)
                record.put("type", message.getType().toString());
            record.put("consumerGroupIdentifier", message.getConsumerGroupIdentifier());
            record.put("messageId", message.getMessageId());
            record.put("conversationId", message.getConversationId());
            record.put("senderId", message.getSender());
            record.put("receiverId", message.getReceiver());
            record.put("timestamp", message.getTimestamp());
            record.put("rawPayload", ByteBuffer.wrap(message.getRawPayload()));

            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
                writer.write(record, encoder);
                encoder.flush();
                return out.toByteArray();
            } catch (IOException e) {
                // Encoding to an in-memory ByteArrayOutputStream cannot realistically fail;
                // wrapped only to satisfy the checked-exception signature of DatumWriter#write.
                throw new IllegalStateException("Unexpected failure encoding ChatMessage to Avro", e);
            }
        } else
            throw new IllegalArgumentException("directMessage is acceptable for DirectMessageByteEncoder");
    }
}

