package sbp.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import sbp.util.HashSum;

import java.io.IOException;
import java.util.logging.Logger;

public class HashSumDeserializer implements Deserializer<HashSum> {

    private static final Logger logger = Logger.getLogger(HashSumDeserializer.class.getName());

    @Override
    public HashSum deserialize(String topic, byte[] data) {
        if(data != null) {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                return objectMapper.readValue(data, HashSum.class);
            } catch (IOException e) {
                logger.severe("JsonProcessingException: " + e.getMessage());
                throw new SerializationException(e);
            }
        }
        throw new SerializationException("Не получилось десериализовать значение потому что оно null");
    }
}
