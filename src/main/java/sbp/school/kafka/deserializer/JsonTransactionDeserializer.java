package sbp.school.kafka.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import sbp.school.kafka.util.Transaction;
import sbp.school.kafka.validation.ValidationService;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class JsonTransactionDeserializer implements Deserializer<Transaction> {
    @Override
    public Transaction deserialize(String topic, byte[] data) {
        if(data != null) {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm"));
            objectMapper.registerModule(new JavaTimeModule());
            try {
                ValidationService.validateBySchema(objectMapper.readTree(data));
                return objectMapper.readValue(data, Transaction.class);
            } catch (IOException e) {
                System.out.println("JsonProcessingException: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }
        throw new RuntimeException("Получили null в значении");
    }
}
