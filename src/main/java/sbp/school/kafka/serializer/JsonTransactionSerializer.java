package sbp.school.kafka.serializer;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import sbp.school.kafka.util.Transaction;

import java.nio.charset.StandardCharsets;

public class JsonTransactionSerializer implements Serializer<Transaction> {
    @Override
    public byte[] serialize(String topic, Transaction data) {
        if(data != null) {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                return objectMapper.writeValueAsString(data).getBytes(StandardCharsets.UTF_8);
            } catch (JsonProcessingException e) {
                System.out.println("JsonProcessingException: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }
        return null;
    }
}
