package json;

import static json.Json.fromJsonValue;
import static json.Json.isClassPresent;
import static json.Json.raw;
import static json.Json.toCamelCase;
import static json.Json.toSnakeCase;
import static json.Json.typeBetween;

import com.google.protobuf.Any;
import com.google.protobuf.AnyOrBuilder;
import com.google.protobuf.BoolValue;
import com.google.protobuf.BoolValueOrBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.BytesValueOrBuilder;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.DoubleValueOrBuilder;
import com.google.protobuf.DurationOrBuilder;
import com.google.protobuf.Empty;
import com.google.protobuf.EmptyOrBuilder;
import com.google.protobuf.FieldMask;
import com.google.protobuf.FieldMaskOrBuilder;
import com.google.protobuf.FloatValue;
import com.google.protobuf.FloatValueOrBuilder;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int32ValueOrBuilder;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Int64ValueOrBuilder;
import com.google.protobuf.LazyStringArrayList;
import com.google.protobuf.ListValue;
import com.google.protobuf.ListValueOrBuilder;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.NullValue;
import com.google.protobuf.ProtocolMessageEnum;
import com.google.protobuf.ProtocolStringList;
import com.google.protobuf.StringValue;
import com.google.protobuf.StringValueOrBuilder;
import com.google.protobuf.Struct;
import com.google.protobuf.StructOrBuilder;
import com.google.protobuf.TimestampOrBuilder;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt32ValueOrBuilder;
import com.google.protobuf.UInt64Value;
import com.google.protobuf.UInt64ValueOrBuilder;
import com.google.protobuf.Value;
import com.google.protobuf.ValueOrBuilder;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.net.JarURLConnection;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * A codec for serializing and deserializing Protocol Buffers messages to and from JSON.
 *
 * @author Freeman
 * @since 0.2.0
 */
public final class ProtobufCodec implements Json.Codec {

    private static final boolean PROTOBUF_PRESENT = isClassPresent("com.google.protobuf.Message");
    private static final Map<String, Descriptors.Descriptor> TYPE_REGISTRY = new ConcurrentHashMap<>();
    private static final Map<String, Class<? extends Message>> TYPE_CLASS_REGISTRY = new ConcurrentHashMap<>();

    static {
        if (PROTOBUF_PRESENT) {
            initializeTypeRegistry();
        }
    }

    public ProtobufCodec() {}

    @Override
    public boolean canSerialize(Object o) {
        return PROTOBUF_PRESENT && isProtobuf(o);
    }

    @Override
    public void serialize(Json.Writer writer, Object o) {
        writeProtobuf(writer, o);
    }

    @Override
    public boolean canDeserialize(Json.JsonValue jv, Type targetType) {
        return PROTOBUF_PRESENT && isProtobufClass(raw(targetType));
    }

    @Override
    public Object deserialize(Json.JsonValue jv, Type targetType) {
        return parseProtobuf(jv, raw(targetType));
    }

    static boolean isProtobuf(Object o) {
        return isProtobufClass(o.getClass());
    }

    static boolean isSpecialType(Object o) {
        return isSpecialTypeClass(o.getClass());
    }

    static boolean isEnum(Object o) {
        return isEnumClass(o.getClass());
    }

    static boolean isMessageOrBuilder(Object o) {
        return isMessageOrBuilderClass(o.getClass());
    }

    static boolean isProtobufClass(Class<?> raw) {
        return isMessageOrBuilderClass(raw) || isEnumClass(raw) || isSpecialTypeClass(raw);
    }

    static boolean isSpecialTypeClass(Class<?> raw) {
        return typeBetween(raw, null, ProtocolStringList.class);
    }

    static boolean isMessageOrBuilderClass(Class<?> raw) {
        return typeBetween(raw, null, MessageOrBuilder.class);
    }

    static boolean isMessageClass(Class<?> raw) {
        return typeBetween(raw, null, Message.class);
    }

    static boolean isMessageBuilderClass(Class<?> raw) {
        return typeBetween(raw, null, Message.Builder.class);
    }

    static boolean isEnumClass(Class<?> raw) {
        return typeBetween(raw, null, ProtocolMessageEnum.class)
                || typeBetween(raw, null, Descriptors.EnumValueDescriptor.class);
    }

    static void writeProtobuf(Json.Writer writer, Object o) {
        if (isMessageOrBuilder(o)) writeMessageOrBuilder(writer, (MessageOrBuilder) o);
        else if (isEnum(o)) writeEnum(writer, o);
        else if (isSpecialType(o)) writeSpecialType(writer, o);
        else
            throw new Json.WriteException(
                    "Not a protobuf Message, Builder, Enum, or special type: "
                            + o.getClass().getName(),
                    null);
    }

    static void writeMessageOrBuilder(Json.Writer writer, MessageOrBuilder message) {
        if (writeWellKnown(writer, message)) return;
        writer.out.append('{');
        boolean first = true;
        for (var field : message.getDescriptorForType().getFields()) {
            if (unsetOptionalField(message, field)) continue;
            Object v = invokeGetter(message, field);
            if (!first) writer.out.append(',');
            first = false;
            writer.writeString(field.getJsonName());
            writer.out.append(':');
            writer.write(v);
        }
        writer.out.append('}');
    }

    static boolean unsetOptionalField(MessageOrBuilder message, Descriptors.FieldDescriptor field) {
        if (field.isOptional()) {
            // Sync with
            // util.JsonFormat.PrinterImpl.print(MessageOrBuilder,
            // java.lang.String)
            if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE && !message.hasField(field)) {
                // Always skip empty optional message fields. If not we will recurse indefinitely if
                // a message has itself as a sub-field.
                return true;
            }
            // Skip all oneof fields except the one that is actually set
            return field.getContainingOneof() != null && !message.hasField(field);
        }
        return false;
    }

    static void writeEnum(Json.Writer writer, Object e) {
        if (!(e instanceof ProtocolMessageEnum) && !(e instanceof Descriptors.EnumValueDescriptor)) {
            throw new Json.WriteException("Not a protobuf Enum: " + e.getClass().getName());
        }

        Descriptors.EnumValueDescriptor evd =
                e instanceof ProtocolMessageEnum pme ? pme.getValueDescriptor() : (Descriptors.EnumValueDescriptor) e;

        if (evd.getFullName().equals(NullValue.getDescriptor().getFullName())) {
            writer.write(null);
        } else {
            writer.write(evd.getName());
        }
    }

    static void writeSpecialType(Json.Writer writer, Object o) {
        if (o instanceof ProtocolStringList list) {
            writer.write(List.copyOf(list));
        } else {
            throw new Json.WriteException(
                    "Unsupported special protobuf type: " + o.getClass().getName());
        }
    }

    static Object parseProtobuf(Json.JsonValue json, Class<?> raw) {
        if (isMessageClass(raw)) return parseMessage(json, raw);
        if (isMessageBuilderClass(raw)) return parseMessageBuilder(json, raw);
        if (isEnumClass(raw)) return parseEnum(json, raw);
        if (isSpecialTypeClass(raw)) return parseSpecialType(json, raw);
        throw new Json.ConversionException("Not a protobuf Message, Builder, or Enum class: " + raw.getName());
    }

    static Object parseMessage(Json.JsonValue json, Class<?> raw) {
        var builder = newBuilder(raw);
        mergeIntoBuilder(json, builder);
        return builder.build();
    }

    static Object parseMessageBuilder(Json.JsonValue json, Class<?> raw) {
        var builder = newBuilder(raw);
        mergeIntoBuilder(json, builder);
        return builder;
    }

    static boolean writeWellKnown(Json.Writer writer, MessageOrBuilder message) {
        if (message instanceof TimestampOrBuilder t) {
            writer.write(Instant.ofEpochSecond(t.getSeconds(), t.getNanos()).toString());
            return true;
        }
        if (message instanceof DurationOrBuilder d) {
            writer.write(Duration.ofSeconds(d.getSeconds(), d.getNanos()).toString());
            return true;
        }
        if (message instanceof StringValueOrBuilder s) {
            writer.write(s.getValue());
            return true;
        }
        if (message instanceof BytesValueOrBuilder b) {
            writer.write(Base64.getEncoder().encodeToString(b.getValue().toByteArray()));
            return true;
        }
        if (message instanceof BoolValueOrBuilder b) {
            writer.write(b.getValue());
            return true;
        }
        if (message instanceof DoubleValueOrBuilder d) {
            writer.write(d.getValue());
            return true;
        }
        if (message instanceof FloatValueOrBuilder f) {
            writer.write(f.getValue());
            return true;
        }
        if (message instanceof Int32ValueOrBuilder i) {
            writer.write(i.getValue());
            return true;
        }
        if (message instanceof UInt32ValueOrBuilder u32) {
            writer.write(u32.getValue());
            return true;
        }
        if (message instanceof Int64ValueOrBuilder i64) {
            writer.write(i64.getValue());
            return true;
        }
        if (message instanceof UInt64ValueOrBuilder u64) {
            writer.write(u64.getValue());
            return true;
        }
        if (message instanceof FieldMaskOrBuilder mask) {
            writer.write(String.join(",", mask.getPathsList()));
            return true;
        }
        if (message instanceof StructOrBuilder struct) {
            writer.write(structToJsonObject(struct));
            return true;
        }
        if (message instanceof ListValueOrBuilder list) {
            writer.write(listToJsonArray(list));
            return true;
        }
        if (message instanceof ValueOrBuilder value) {
            writer.write(valueToJsonValue(value));
            return true;
        }
        if (message instanceof AnyOrBuilder any) {
            writeAny(writer, any);
            return true;
        }
        if (message instanceof EmptyOrBuilder) {
            writer.write(new Json.JsonObject(Map.of()));
            return true;
        }
        return false;
    }

    static void writeAny(Json.Writer writer, AnyOrBuilder any) {
        Map<String, Json.JsonValue> map = new LinkedHashMap<>();
        map.put("@type", new Json.JsonString(any.getTypeUrl()));

        try {
            // Unpack the Any and serialize the nested message
            String typeUrl = any.getTypeUrl();
            Descriptors.Descriptor descriptor = TYPE_REGISTRY.get(typeUrl);

            if (descriptor == null) {
                throw new Json.ConversionException(
                        "Type not found in registry: " + typeUrl + ". Make sure the message type is on the classpath.");
            }

            // Parse the value using the descriptor - this gives us a DynamicMessage
            Message message = com.google.protobuf.DynamicMessage.parseFrom(descriptor, any.getValue());

            // Manually serialize the DynamicMessage fields
            Json.JsonValue value = serializeDynamicMessage(message);

            // Add all fields from the nested message except those that would conflict with @type
            if (value instanceof Json.JsonObject nestedObj) {
                for (var entry : nestedObj.value().entrySet()) {
                    if (!entry.getKey().equals("@type")) {
                        map.put(entry.getKey(), entry.getValue());
                    }
                }
            } else {
                // For well-known types that serialize to primitives, use "value" field
                map.put("value", value);
            }
        } catch (Exception e) {
            throw new Json.ConversionException("Failed to serialize Any type: " + any.getTypeUrl(), e);
        }

        writer.write(new Json.JsonObject(map));
    }

    /**
     * Serialize a DynamicMessage to JsonValue by manually accessing its fields.
     * This is needed because DynamicMessage doesn't have typed getters.
     */
    private static Json.JsonValue serializeDynamicMessage(Message message) {
        // Check if it's a well-known type first
        var descriptor = message.getDescriptorForType();
        String fullName = descriptor.getFullName();

        // Handle well-known types that serialize to primitives
        if (fullName.equals("google.protobuf.Timestamp")) {
            var seconds = (Long) message.getField(descriptor.findFieldByName("seconds"));
            var nanos = (Integer) message.getField(descriptor.findFieldByName("nanos"));
            return new Json.JsonString(Instant.ofEpochSecond(seconds, nanos).toString());
        }
        if (fullName.equals("google.protobuf.Duration")) {
            var seconds = (Long) message.getField(descriptor.findFieldByName("seconds"));
            var nanos = (Integer) message.getField(descriptor.findFieldByName("nanos"));
            return new Json.JsonString(Duration.ofSeconds(seconds, nanos).toString());
        }
        if (fullName.equals("google.protobuf.StringValue")) {
            return new Json.JsonString((String) message.getField(descriptor.findFieldByName("value")));
        }
        if (fullName.equals("google.protobuf.Int32Value")) {
            return new Json.JsonNumber((Integer) message.getField(descriptor.findFieldByName("value")));
        }
        if (fullName.equals("google.protobuf.Int64Value")) {
            return new Json.JsonNumber((Long) message.getField(descriptor.findFieldByName("value")));
        }
        if (fullName.equals("google.protobuf.UInt32Value")) {
            return new Json.JsonNumber((Integer) message.getField(descriptor.findFieldByName("value")));
        }
        if (fullName.equals("google.protobuf.UInt64Value")) {
            return new Json.JsonNumber((Long) message.getField(descriptor.findFieldByName("value")));
        }
        if (fullName.equals("google.protobuf.BoolValue")) {
            return new Json.JsonBoolean((Boolean) message.getField(descriptor.findFieldByName("value")));
        }
        if (fullName.equals("google.protobuf.FloatValue")) {
            return new Json.JsonNumber((Float) message.getField(descriptor.findFieldByName("value")));
        }
        if (fullName.equals("google.protobuf.DoubleValue")) {
            return new Json.JsonNumber((Double) message.getField(descriptor.findFieldByName("value")));
        }

        // For regular messages, serialize all fields to a JSON object
        Map<String, Json.JsonValue> fieldMap = new LinkedHashMap<>();
        for (var field : descriptor.getFields()) {
            // Use the same logic as unsetOptionalField to skip fields
            if (unsetOptionalField(message, field)) {
                continue;
            }
            Object fieldValue = message.getField(field);
            fieldMap.put(field.getJsonName(), convertFieldValue(fieldValue, field));
        }
        return new Json.JsonObject(fieldMap);
    }

    /**
     * Convert a protobuf field value to JsonValue.
     */
    private static Json.JsonValue convertFieldValue(Object value, Descriptors.FieldDescriptor field) {
        if (field.isRepeated()) {
            List<Json.JsonValue> list = new ArrayList<>();
            for (Object item : (List<?>) value) {
                list.add(convertSingleValue(item, field));
            }
            return new Json.JsonArray(list);
        } else {
            return convertSingleValue(value, field);
        }
    }

    /**
     * Convert a single protobuf value to JsonValue.
     */
    private static Json.JsonValue convertSingleValue(Object value, Descriptors.FieldDescriptor field) {
        return switch (field.getType()) {
            case INT32, SINT32, SFIXED32, UINT32, FIXED32 -> new Json.JsonNumber((Integer) value);
            case INT64, SINT64, SFIXED64, UINT64, FIXED64 -> new Json.JsonNumber((Long) value);
            case FLOAT -> new Json.JsonNumber((Float) value);
            case DOUBLE -> new Json.JsonNumber((Double) value);
            case BOOL -> new Json.JsonBoolean((Boolean) value);
            case STRING -> new Json.JsonString((String) value);
            case BYTES -> new Json.JsonString(Base64.getEncoder().encodeToString(((ByteString) value).toByteArray()));
            case ENUM -> new Json.JsonString(((Descriptors.EnumValueDescriptor) value).getName());
            case MESSAGE -> serializeDynamicMessage((Message) value);
            default -> throw new Json.ConversionException("Unsupported field type: " + field.getType());
        };
    }

    static Json.JsonObject structToJsonObject(StructOrBuilder struct) {
        Map<String, Json.JsonValue> map = new LinkedHashMap<>();
        for (var entry : struct.getFieldsMap().entrySet()) {
            map.put(entry.getKey(), valueToJsonValue(entry.getValue()));
        }
        return new Json.JsonObject(map);
    }

    static Json.JsonArray listToJsonArray(ListValueOrBuilder listValue) {
        List<Json.JsonValue> list = new ArrayList<>();
        for (Value v : listValue.getValuesList()) list.add(valueToJsonValue(v));
        return new Json.JsonArray(list);
    }

    static Json.JsonValue valueToJsonValue(ValueOrBuilder value) {
        return switch (value.getKindCase()) {
            case NULL_VALUE -> new Json.JsonNull();
            case NUMBER_VALUE -> new Json.JsonNumber(value.getNumberValue());
            case STRING_VALUE -> new Json.JsonString(value.getStringValue());
            case BOOL_VALUE -> new Json.JsonBoolean(value.getBoolValue());
            case STRUCT_VALUE -> structToJsonObject(value.getStructValue());
            case LIST_VALUE -> listToJsonArray(value.getListValue());
            case KIND_NOT_SET -> new Json.JsonNull(); // maybe should throw?
        };
    }

    static void mergeIntoBuilder(Json.JsonValue json, Message.Builder builder) {
        if (mergeWellKnown(json, builder)) return;

        // special types here, like Date, DateTime

        Json.JsonObject object = expectObject(json);
        var descriptor = builder.getDescriptorForType();
        var fieldMap = buildFieldMap(descriptor);
        for (var entry : object.value().entrySet()) {
            var field = fieldMap.get(entry.getKey());
            if (field != null) {
                var getterType = getterType(builder, field);
                Object v = fromJsonValue(entry.getValue(), getterType);
                try {
                    if (field.isMapField()) {
                        putAllMethod(builder, field, Map.class).invoke(builder, v);
                    } else if (field.isRepeated()) {
                        addAllMethod(builder, field, Iterable.class).invoke(builder, v);
                    } else {
                        setterMethod(builder, field, raw(getterType)).invoke(builder, v);
                    }
                } catch (Exception e) {
                    throw new Json.ConversionException(
                            "Cannot set field '" + field.getFullName() + "' on builder "
                                    + builder.getClass().getName(),
                            e);
                }
            }
        }
    }

    static java.lang.reflect.Type getterType(Message.Builder builder, Descriptors.FieldDescriptor field) {
        var getterMethodName = getterMethodName(field);
        try {
            var m = builder.getClass().getMethod(getterMethodName);
            return m.getGenericReturnType();
        } catch (NoSuchMethodException e) {
            throw new Json.ConversionException(
                    "Cannot find getter method '" + getterMethodName + "' on "
                            + builder.getClass().getName(),
                    e);
        }
    }

    static Method setterMethod(Message.Builder builder, Descriptors.FieldDescriptor field, Class<?>... params) {
        var snake = toCamelCase(field.getName());
        var setter = "set" + Character.toUpperCase(snake.charAt(0)) + snake.substring(1);
        try {
            return builder.getClass().getMethod(setter, params);
        } catch (NoSuchMethodException e) {
            throw new Json.ConversionException(
                    "Cannot find setter method '" + setter + "' on "
                            + builder.getClass().getName(),
                    e);
        }
    }

    static Method addAllMethod(Message.Builder builder, Descriptors.FieldDescriptor field, Class<?>... params) {
        var snake = toCamelCase(field.getName());
        var adder = "addAll" + Character.toUpperCase(snake.charAt(0)) + snake.substring(1);
        try {
            return builder.getClass().getMethod(adder, params);
        } catch (NoSuchMethodException e) {
            throw new Json.ConversionException(
                    "Cannot find addAll method '" + adder + "' on "
                            + builder.getClass().getName(),
                    e);
        }
    }

    static Method putAllMethod(Message.Builder builder, Descriptors.FieldDescriptor field, Class<?>... params) {
        var snake = toCamelCase(field.getName());
        var putter = "putAll" + Character.toUpperCase(snake.charAt(0)) + snake.substring(1);
        try {
            return builder.getClass().getMethod(putter, params);
        } catch (NoSuchMethodException e) {
            throw new Json.ConversionException(
                    "Cannot find putAll method '" + putter + "' on "
                            + builder.getClass().getName(),
                    e);
        }
    }

    private static String getterMethodName(Descriptors.FieldDescriptor field) {
        var snake = toCamelCase(field.getName());
        String getterMethodName;
        if (field.isMapField()) {
            getterMethodName = "get" + Character.toUpperCase(snake.charAt(0)) + snake.substring(1) + "Map";
        } else if (field.isRepeated()) {
            getterMethodName = "get" + Character.toUpperCase(snake.charAt(0)) + snake.substring(1) + "List";
        } else {
            getterMethodName = "get" + Character.toUpperCase(snake.charAt(0)) + snake.substring(1);
        }
        return getterMethodName;
    }

    static Object invokeGetter(MessageOrBuilder messageOrBuilder, Descriptors.FieldDescriptor field) {
        var getter = getterMethodName(field);
        Method method;
        try {
            method = messageOrBuilder.getClass().getMethod(getter);
        } catch (NoSuchMethodException e) {
            throw new Json.ConversionException(
                    "Cannot find getter method '" + getter + "' on "
                            + messageOrBuilder.getClass().getName(),
                    e);
        }
        try {
            return method.invoke(messageOrBuilder);
        } catch (Exception e) {
            throw new Json.ConversionException(
                    "Cannot invoke getter method '" + getter + "' on "
                            + messageOrBuilder.getClass().getName(),
                    e);
        }
    }

    static boolean mergeWellKnown(Json.JsonValue json, Message.Builder builder) {
        if (builder instanceof com.google.protobuf.Timestamp.Builder timestamp) {
            Instant instant = fromJsonValue(json, Instant.class);
            timestamp.setSeconds(instant.getEpochSecond());
            timestamp.setNanos(instant.getNano());
            return true;
        }
        if (builder instanceof com.google.protobuf.Duration.Builder duration) {
            Duration dur = fromJsonValue(json, Duration.class);
            duration.setSeconds(dur.getSeconds());
            duration.setNanos(dur.getNano());
            return true;
        }
        if (builder instanceof StringValue.Builder stringValue) {
            stringValue.setValue(fromJsonValue(json, String.class));
            return true;
        }
        if (builder instanceof BytesValue.Builder bytesValue) {
            byte[] bytes = Base64.getDecoder().decode((String) fromJsonValue(json, String.class));
            bytesValue.setValue(ByteString.copyFrom(bytes));
            return true;
        }
        if (builder instanceof BoolValue.Builder boolValue) {
            boolValue.setValue(fromJsonValue(json, Boolean.class));
            return true;
        }
        if (builder instanceof DoubleValue.Builder doubleValue) {
            doubleValue.setValue(fromJsonValue(json, Double.class));
            return true;
        }
        if (builder instanceof FloatValue.Builder floatValue) {
            floatValue.setValue(fromJsonValue(json, Float.class));
            return true;
        }
        if (builder instanceof Int32Value.Builder int32Value) {
            int32Value.setValue(fromJsonValue(json, Integer.class));
            return true;
        }
        if (builder instanceof UInt32Value.Builder uint32Value) {
            uint32Value.setValue(fromJsonValue(json, Integer.class));
            return true;
        }
        if (builder instanceof Int64Value.Builder int64Value) {
            int64Value.setValue(fromJsonValue(json, Long.class));
            return true;
        }
        if (builder instanceof UInt64Value.Builder uint64Value) {
            uint64Value.setValue(fromJsonValue(json, Long.class));
            return true;
        }
        if (builder instanceof FieldMask.Builder fieldMask) {
            String str = fromJsonValue(json, String.class);
            for (var s : str.split(",")) fieldMask.addPaths(s);
            return true;
        }
        if (builder instanceof Struct.Builder struct) {
            for (var entry : expectObject(json).value().entrySet()) {
                struct.putFields(entry.getKey(), toValue(entry.getValue()));
            }
            return true;
        }
        if (builder instanceof ListValue.Builder list) {
            var ja = json instanceof Json.JsonArray ? (Json.JsonArray) json : new Json.JsonArray(List.of(json));
            for (var v : ja.value()) list.addValues(toValue(v));
            return true;
        }
        if (builder instanceof Value.Builder value) {
            value.mergeFrom(toValue(json));
            return true;
        }
        if (builder instanceof Any.Builder any) {
            parseAny(json, any);
            return true;
        }
        if (builder instanceof Empty.Builder) {
            return true;
        }
        return false;
    }

    static void parseAny(Json.JsonValue json, Any.Builder anyBuilder) {
        Json.JsonObject obj = expectObject(json);

        // Extract @type field
        Json.JsonValue typeValue = obj.value().get("@type");
        if (typeValue == null) {
            throw new Json.ConversionException("Any type must have @type field");
        }
        if (!(typeValue instanceof Json.JsonString typeStr)) {
            throw new Json.ConversionException("@type field must be a string");
        }

        String typeUrl = typeStr.value();
        Class<? extends Message> messageClass = TYPE_CLASS_REGISTRY.get(typeUrl);

        if (messageClass == null) {
            throw new Json.ConversionException(
                    "Type not found in registry: " + typeUrl + ". Make sure the message type is on the classpath.");
        }

        try {
            // Create a JSON object with all fields except @type
            Map<String, Json.JsonValue> fields = new LinkedHashMap<>();
            for (var entry : obj.value().entrySet()) {
                if (!entry.getKey().equals("@type")) {
                    fields.put(entry.getKey(), entry.getValue());
                }
            }

            // If there's only a "value" field, use it directly (for well-known types)
            Json.JsonValue valueToDeserialize;
            if (fields.size() == 1 && fields.containsKey("value")) {
                valueToDeserialize = fields.get("value");
            } else {
                valueToDeserialize = new Json.JsonObject(fields);
            }

            // Parse the nested message using the actual Message class
            Message.Builder nestedBuilder = newBuilder(messageClass);
            mergeIntoBuilder(valueToDeserialize, nestedBuilder);
            Message nestedMessage = nestedBuilder.build();

            // Pack into Any
            anyBuilder.setTypeUrl(typeUrl);
            anyBuilder.setValue(nestedMessage.toByteString());
        } catch (Exception e) {
            throw new Json.ConversionException("Failed to deserialize Any type: " + typeUrl, e);
        }
    }

    static Object parseEnum(Json.JsonValue jv, Class<?> raw) {
        if (!typeBetween(raw, null, Enum.class))
            throw new Json.ConversionException("Not a protobuf Enum class: " + raw.getName());
        if (raw == NullValue.class) { // special case
            if (jv instanceof Json.JsonNull) return NullValue.NULL_VALUE;
            if (jv instanceof Json.JsonString js) {
                if (js.value().equalsIgnoreCase(NullValue.NULL_VALUE.name())) return NullValue.NULL_VALUE;
            }
            if (jv instanceof Json.JsonNumber jn) {
                if (jn.value().intValue() == NullValue.NULL_VALUE.getNumber()) return NullValue.NULL_VALUE;
            }
            throw new Json.ConversionException("Cannot convert " + jv.getClass() + " to NullValue");
        }
        if (!(jv instanceof Json.JsonString) && !(jv instanceof Json.JsonNumber)) {
            jv = new Json.JsonString(Json.toString(jv));
        }
        if (jv instanceof Json.JsonString s) {
            for (Object ec : raw.getEnumConstants()) if (((Enum<?>) ec).name().equalsIgnoreCase(s.value())) return ec;
            throw new Json.ConversionException("No enum constant " + raw.getName() + "." + s.value());
        }
        if (jv instanceof Json.JsonNumber n) {
            var i = n.value().intValue();
            for (Object constant : raw.getEnumConstants()) {
                var pm = (ProtocolMessageEnum) constant;
                if (i == -1 && Objects.equals(pm.getValueDescriptor().getName(), "UNRECOGNIZED")) return pm;
                if (pm.getNumber() == i) return pm;
            }
            throw new Json.ConversionException("No enum constant " + raw.getName() + " with number " + i);
        }
        throw new Json.ConversionException(
                "Cannot coerce " + jv.getClass().getSimpleName() + " to enum " + raw.getName());
    }

    static Object parseSpecialType(Json.JsonValue jv, Class<?> raw) {
        if (typeBetween(raw, LazyStringArrayList.class, ProtocolStringList.class)) {
            List<String> list = fromJsonValue(jv, new Json.Type<List<String>>() {}.getType());
            return new LazyStringArrayList(list);
        }
        throw new Json.ConversionException("Not a supported type: " + raw.getName());
    }

    static Message.Builder newBuilder(Class<?> raw) {
        Class<?> targetClass = raw;
        if (isMessageBuilderClass(raw)) {
            var enclosing = raw.getEnclosingClass();
            if (enclosing != null && isMessageClass(enclosing)) targetClass = enclosing;
        }
        try {
            var method = targetClass.getMethod("newBuilder");
            return (Message.Builder) method.invoke(null);
        } catch (Exception e) {
            throw new Json.ConversionException("Cannot create protobuf builder for " + targetClass.getName(), e);
        }
    }

    static Map<String, Descriptors.FieldDescriptor> buildFieldMap(Descriptors.Descriptor descriptor) {
        Map<String, Descriptors.FieldDescriptor> map = new LinkedHashMap<>();
        for (var field : descriptor.getFields()) {
            map.put(field.getName(), field);
            String snake = toSnakeCase(field.getName());
            if (!snake.equals(field.getName())) map.put(snake, field);
            String camel = toCamelCase(field.getName());
            if (!camel.equals(field.getName())) map.put(camel, field);
            if (!Objects.equals(field.getName(), field.getJsonName())) {
                map.put(field.getJsonName(), field);
                String snakeJson = toSnakeCase(field.getJsonName());
                if (!snakeJson.equals(field.getJsonName())) map.put(snakeJson, field);
                String camelJson = toCamelCase(field.getJsonName());
                if (!camelJson.equals(field.getJsonName())) map.put(camelJson, field);
            }
        }
        return map;
    }

    static Json.JsonObject expectObject(Json.JsonValue value) {
        if (value instanceof Json.JsonObject obj) return obj;
        throw new Json.ConversionException("Expected JSON object for protobuf message, but got "
                + value.getClass().getSimpleName());
    }

    /**
     * Initialize the type registry by scanning the classpath for all protobuf Message types.
     * This allows automatic handling of Any types without manual TypeRegistry configuration.
     */
    private static void initializeTypeRegistry() {
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            if (classLoader == null) {
                classLoader = ProtobufCodec.class.getClassLoader();
            }

            // Scan common protobuf packages
            String[] packagesToScan = {"com.google", "json4j"};
            for (String pkg : packagesToScan) {
                scanPackage(classLoader, pkg);
            }
        } catch (Exception e) {
            // Silently ignore - type registry will just be empty
            // Any types that aren't registered will fail at runtime with a clear error
        }
    }

    private static void scanPackage(ClassLoader classLoader, String packageName) {
        try {
            String path = packageName.replace('.', '/');
            Enumeration<URL> resources = classLoader.getResources(path);

            while (resources.hasMoreElements()) {
                URL resource = resources.nextElement();
                if (resource.getProtocol().equals("jar")) {
                    scanJar(resource, packageName);
                } else if (resource.getProtocol().equals("file")) {
                    scanDirectory(new File(resource.getFile()), packageName);
                }
            }
        } catch (IOException e) {
            // Silently ignore
        }
    }

    private static void scanJar(URL resource, String packageName) {
        try {
            JarURLConnection connection = (JarURLConnection) resource.openConnection();
            JarFile jarFile = connection.getJarFile();
            Enumeration<JarEntry> entries = jarFile.entries();

            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String name = entry.getName();

                if (name.endsWith(".class") && name.startsWith(packageName.replace('.', '/'))) {
                    String className = name.replace('/', '.').substring(0, name.length() - 6);
                    registerMessageClass(className);
                }
            }
        } catch (IOException e) {
            // Silently ignore
        }
    }

    private static void scanDirectory(File directory, String packageName) {
        if (!directory.exists() || !directory.isDirectory()) {
            return;
        }

        File[] files = directory.listFiles();
        if (files == null) {
            return;
        }

        for (File file : files) {
            if (file.isDirectory()) {
                scanDirectory(file, packageName + "." + file.getName());
            } else if (file.getName().endsWith(".class")) {
                String className = packageName
                        + '.'
                        + file.getName().substring(0, file.getName().length() - 6);
                registerMessageClass(className);
            }
        }
    }

    private static void registerMessageClass(String className) {
        try {
            Class<?> clazz =
                    Class.forName(className, false, Thread.currentThread().getContextClassLoader());

            // Only register concrete Message classes (not builders, not interfaces, not abstract)
            if (Message.class.isAssignableFrom(clazz)
                    && !clazz.isInterface()
                    && !Modifier.isAbstract(clazz.getModifiers())
                    && !clazz.getName().endsWith("$Builder")) {

                // Get the descriptor via reflection
                Method getDescriptor = clazz.getMethod("getDescriptor");
                if (Modifier.isStatic(getDescriptor.getModifiers())) {
                    Descriptors.Descriptor descriptor = (Descriptors.Descriptor) getDescriptor.invoke(null);
                    String typeUrl = "type.googleapis.com/" + descriptor.getFullName();
                    TYPE_REGISTRY.put(typeUrl, descriptor);
                    TYPE_CLASS_REGISTRY.put(typeUrl, (Class<? extends Message>) clazz);
                }
            }
        } catch (Exception e) {
            // Silently ignore classes that can't be loaded or don't have a descriptor
        }
    }

    static Value toValue(Json.JsonValue value) {
        var builder = Value.newBuilder();
        if (value instanceof Json.JsonNull) builder.setNullValue(NullValue.NULL_VALUE);
        else if (value instanceof Json.JsonBoolean b) builder.setBoolValue(b.value());
        else if (value instanceof Json.JsonNumber n)
            builder.setNumberValue(n.value().doubleValue());
        else if (value instanceof Json.JsonString s) builder.setStringValue(s.value());
        else if (value instanceof Json.JsonObject o) {
            var sb = Struct.newBuilder();
            mergeIntoBuilder(o, sb);
            builder.setStructValue(sb.build());
        } else if (value instanceof Json.JsonArray a) {
            var lb = ListValue.newBuilder();
            mergeIntoBuilder(a, lb);
            builder.setListValue(lb.build());
        }
        return builder.build();
    }
}
