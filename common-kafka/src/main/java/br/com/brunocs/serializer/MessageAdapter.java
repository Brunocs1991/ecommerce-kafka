package br.com.brunocs.serializer;

import br.com.brunocs.kafka.CorrelationId;
import br.com.brunocs.kafka.Message;
import com.google.gson.*;

import java.lang.reflect.Type;

public class MessageAdapter implements JsonSerializer<Message>, JsonDeserializer<Message> {

    @Override
    public JsonElement serialize(Message message, Type type, JsonSerializationContext context) {
        JsonObject object = new JsonObject();
        object.addProperty("type", message.getPayload().getClass().getName());
        object.add("payload",context.serialize(message.getPayload()));
        object.add("correlationId", context.serialize(message.getId()));
        return object;
    }

    @Override
    public Message deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext context) throws JsonParseException {
        var obj = jsonElement.getAsJsonObject();
        var payloadType = obj.get("type").getAsString();
        var correlationId =(CorrelationId) context.deserialize(obj.get("correlationId"), CorrelationId.class);

        try {
            var payload = context.deserialize(obj.get("payload"), Class.forName(payloadType));
            return new Message(correlationId, payload);
        } catch (ClassNotFoundException e) {
            throw new JsonParseException(e);
        }

    }
}
