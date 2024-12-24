package sbp.school.kafka.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import sbp.school.kafka.serializer.JsonTransactionSerializer;

import java.util.Set;
import java.util.stream.Collectors;

public class ValidationService {

    public static void validateBySchema(JsonNode data) {
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
