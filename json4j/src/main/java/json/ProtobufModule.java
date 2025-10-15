package json;

import java.lang.reflect.Type;

/**
 *
 *
 * @author Freeman
 * @since 2025/10/15
 */
class ProtobufModule implements Json.Serializer, Json.Deserializer {

    @Override
    public boolean canSerialize(Object o) {
        return false;
    }

    @Override
    public String serialize(Object o) {
        return "";
    }

    @Override
    public boolean canDeserialize(Json.JsonValue jv, Type targetType) {
        return false;
    }

    @Override
    public Object deserialize(Json.JsonValue jv, Type targetType) {
        return null;
    }
}
