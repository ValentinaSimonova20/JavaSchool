package sbp.school.kafka.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import sbp.school.kafka.util.Transaction;
import sbp.school.kafka.validation.ValidationService;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.logging.Logger;

public class JsonTransactionDeserializer implements Deserializer<Transaction> {

    private static final Logger logger = Logger.getLogger(JsonTransactionDeserializer.class.getName());

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
                logger.severe("JsonProcessingException: " + e.getMessage());
                throw new SerializationException(e);
            }
        }
        throw new SerializationException("Не получилось десериализовать значение потому что оно null");
    }
}
