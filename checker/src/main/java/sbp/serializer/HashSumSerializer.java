package sbp.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import sbp.util.HashSum;

import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

public class HashSumSerializer implements Serializer<HashSum> {

    private static final Logger logger = Logger.getLogger(HashSumSerializer.class.getName());



    @Override
    public byte[] serialize(String topic, HashSum data) {
        if(data != null) {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                return objectMapper.writeValueAsString(data).getBytes(StandardCharsets.UTF_8);
            } catch (JsonProcessingException e) {
                logger.severe("JsonProcessingException: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }
        return new byte[0];
    }
}
