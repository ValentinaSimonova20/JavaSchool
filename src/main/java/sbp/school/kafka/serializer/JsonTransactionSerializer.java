package sbp.school.kafka.serializer;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serializer;
import sbp.school.kafka.util.Transaction;
import sbp.school.kafka.validation.ValidationService;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;

public class JsonTransactionSerializer implements Serializer<Transaction> {
    @Override
    public byte[] serialize(String topic, Transaction data) {
        if(data != null) {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm"));
            objectMapper.registerModule(new JavaTimeModule());
            try {
                String dataString = objectMapper.writeValueAsString(data);
                ValidationService.validateBySchema(objectMapper.readTree(dataString));
                return dataString.getBytes(StandardCharsets.UTF_8);
            } catch (JsonProcessingException e) {
                System.out.println("JsonProcessingException: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }
        return new byte[0];
    }
}
