package sbp.school.kafka.serializer;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.apache.kafka.common.serialization.Serializer;
import sbp.school.kafka.util.Transaction;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Set;
import java.util.stream.Collectors;

public class JsonTransactionSerializer implements Serializer<Transaction> {
    @Override
    public byte[] serialize(String topic, Transaction data) {
        if(data != null) {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm"));
            objectMapper.registerModule(new JavaTimeModule());
            try {
                String dataString = objectMapper.writeValueAsString(data);
                validateBySchema(objectMapper.readTree(dataString));
                return dataString.getBytes(StandardCharsets.UTF_8);
            } catch (JsonProcessingException e) {
                System.out.println("JsonProcessingException: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }
        return new byte[0];
    }

    private void validateBySchema(JsonNode data) {
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V4);
        JsonSchema jsonSchema = factory.getSchema(
                JsonTransactionSerializer.class.getResourceAsStream("/schema/transaction.json")
        );
        Set<ValidationMessage> messages =  jsonSchema.validate(data);
        if(!messages.isEmpty()) {
            throw new RuntimeException("сообщение не проходит по схеме. ошибки: " +
                    String.join(
                            ";",
                            messages.stream().map(ValidationMessage::getMessage)
                                    .collect(Collectors.toSet())
                    )
            );
        }
    }
}
