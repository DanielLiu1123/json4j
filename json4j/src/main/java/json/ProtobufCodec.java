package json;

import static json.Json.isClassPresent;
import static json.Json.mapCap;
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
import com.google.protobuf.Timestamp;
import com.google.protobuf.TimestampOrBuilder;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt32ValueOrBuilder;
import com.google.protobuf.UInt64Value;
import com.google.protobuf.UInt64ValueOrBuilder;
import com.google.protobuf.Value;
import com.google.protobuf.ValueOrBuilder;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarFile;

/**
 * A codec for serializing and deserializing Protocol Buffers messages to and from JSON.
 *
 * @author Freeman
 * @since 0.2.0
 */
public final class ProtobufCodec implements Json.Serializer, Json.Deserializer {

    private static final boolean PROTOBUF_PRESENT = isClassPresent("com.google.protobuf.Message");

    public ProtobufCodec() {}

    @Override
    public boolean canSerialize(Object o) {
        return PROTOBUF_PRESENT && isProtobuf(o);
    }

    @Override
    public String serialize(Json.Writer writer, Object o) {
        return writeProtobuf(writer, o);
    }

    @Override
    public boolean canDeserialize(Json.JsonValue jsonValue, Type targetType) {
        return PROTOBUF_PRESENT && isProtobufClass(raw(targetType));
    }

    @Override
    public Object deserialize(Json.Parser parser, Json.JsonValue jsonValue, Type targetType) {
        return parseProtobuf(parser, jsonValue, raw(targetType));
    }

    static Map<String, Class<?>> getTypeRegistry() {
        return TypeRegistryHolder.INSTANCE;
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

    static String writeProtobuf(Json.Writer writer, Object o) {
        if (isMessageOrBuilder(o)) return writeMessageOrBuilder(writer, (MessageOrBuilder) o);
        else if (isEnum(o)) return writeEnum(writer, o);
        else if (isSpecialType(o)) return writeSpecialType(writer, o);
        else
            throw new Json.WriteException(
                    "Not a protobuf Message, Builder, Enum, or special type: "
                            + o.getClass().getName(),
                    null);
    }

    static String writeMessageOrBuilder(Json.Writer writer, MessageOrBuilder message) {
        if (isWellKnown(message.getDescriptorForType().getFullName())) {
            return writer.write(wellKnownToJsonValue(message));
        }
        var jo = messageToJsonValue(message);
        return writer.write(jo);
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

    static boolean isWellKnown(String fullName) {
        return getWellKnownClass(fullName) != null;
    }

    static Class<?> getWellKnownClass(String fullName) {
        if (fullName.equals(Timestamp.getDescriptor().getFullName())) return Timestamp.class;
        if (fullName.equals(com.google.protobuf.Duration.getDescriptor().getFullName()))
            return com.google.protobuf.Duration.class;
        if (fullName.equals(StringValue.getDescriptor().getFullName())) return StringValue.class;
        if (fullName.equals(Int32Value.getDescriptor().getFullName())) return Int32Value.class;
        if (fullName.equals(Int64Value.getDescriptor().getFullName())) return Int64Value.class;
        if (fullName.equals(UInt32Value.getDescriptor().getFullName())) return UInt32Value.class;
        if (fullName.equals(UInt64Value.getDescriptor().getFullName())) return UInt64Value.class;
        if (fullName.equals(BoolValue.getDescriptor().getFullName())) return BoolValue.class;
        if (fullName.equals(FloatValue.getDescriptor().getFullName())) return FloatValue.class;
        if (fullName.equals(DoubleValue.getDescriptor().getFullName())) return DoubleValue.class;
        if (fullName.equals(BytesValue.getDescriptor().getFullName())) return BytesValue.class;
        if (fullName.equals(FieldMask.getDescriptor().getFullName())) return FieldMask.class;
        if (fullName.equals(Struct.getDescriptor().getFullName())) return Struct.class;
        if (fullName.equals(ListValue.getDescriptor().getFullName())) return ListValue.class;
        if (fullName.equals(Value.getDescriptor().getFullName())) return Value.class;
        if (fullName.equals(Empty.getDescriptor().getFullName())) return Empty.class;
        if (fullName.equals(Any.getDescriptor().getFullName())) return Any.class;
        return null;
    }

    static String writeEnum(Json.Writer writer, Object e) {
        if (!(e instanceof ProtocolMessageEnum) && !(e instanceof Descriptors.EnumValueDescriptor)) {
            throw new Json.WriteException("Not a protobuf Enum: " + e.getClass().getName());
        }

        Descriptors.EnumValueDescriptor evd =
                e instanceof ProtocolMessageEnum pme ? pme.getValueDescriptor() : (Descriptors.EnumValueDescriptor) e;

        if (evd.getFullName().equals(NullValue.getDescriptor().getFullName())) {
            return writer.write(null);
        } else {
            return writer.write(evd.getName());
        }
    }

    static String writeSpecialType(Json.Writer writer, Object o) {
        if (o instanceof ProtocolStringList list) {
            return writer.write(List.copyOf(list));
        } else {
            throw new Json.WriteException(
                    "Unsupported special protobuf type: " + o.getClass().getName());
        }
    }

    static Object parseProtobuf(Json.Parser parser, Json.JsonValue json, Class<?> raw) {
        if (isMessageClass(raw)) return parseMessage(parser, json, raw);
        if (isMessageBuilderClass(raw)) return parseMessageBuilder(parser, json, raw);
        if (isEnumClass(raw)) return parseEnum(json, raw);
        if (isSpecialTypeClass(raw)) return parseSpecialType(parser, json, raw);
        throw new Json.ConversionException("Not a protobuf Message, Builder, or Enum class: " + raw.getName());
    }

    static Object parseMessage(Json.Parser parser, Json.JsonValue json, Class<?> raw) {
        var builder = newBuilder(raw);
        mergeIntoBuilder(parser, json, builder);
        return builder.build();
    }

    static Object parseMessageBuilder(Json.Parser parser, Json.JsonValue json, Class<?> raw) {
        var builder = newBuilder(raw);
        mergeIntoBuilder(parser, json, builder);
        return builder;
    }

    static Json.JsonValue wellKnownToJsonValue(MessageOrBuilder message) {
        if (message instanceof TimestampOrBuilder t) {
            return new Json.JsonString(
                    Instant.ofEpochSecond(t.getSeconds(), t.getNanos()).toString());
        } else if (message instanceof DurationOrBuilder d) {
            return new Json.JsonString(
                    Duration.ofSeconds(d.getSeconds(), d.getNanos()).toString());
        } else if (message instanceof StringValueOrBuilder s) {
            return new Json.JsonString(s.getValue());
        } else if (message instanceof BytesValueOrBuilder b) {
            return new Json.JsonString(
                    Base64.getEncoder().encodeToString(b.getValue().toByteArray()));
        } else if (message instanceof BoolValueOrBuilder b) {
            return new Json.JsonBoolean(b.getValue());
        } else if (message instanceof DoubleValueOrBuilder d) {
            return new Json.JsonNumber(d.getValue());
        } else if (message instanceof FloatValueOrBuilder f) {
            return new Json.JsonNumber(f.getValue());
        } else if (message instanceof Int32ValueOrBuilder i) {
            return new Json.JsonNumber(i.getValue());
        } else if (message instanceof UInt32ValueOrBuilder u32) {
            return new Json.JsonNumber(u32.getValue());
        } else if (message instanceof Int64ValueOrBuilder i64) {
            return new Json.JsonNumber(i64.getValue());
        } else if (message instanceof UInt64ValueOrBuilder u64) {
            return new Json.JsonNumber(u64.getValue());
        } else if (message instanceof FieldMaskOrBuilder mask) {
            return new Json.JsonString(String.join(",", mask.getPathsList()));
        } else if (message instanceof StructOrBuilder struct) {
            return structToJsonObject(struct);
        } else if (message instanceof ListValueOrBuilder list) {
            return listToJsonArray(list);
        } else if (message instanceof ValueOrBuilder value) {
            return valueToJsonValue(value);
        } else if (message instanceof EmptyOrBuilder) {
            return new Json.JsonObject(Map.of());
        } else if (message instanceof AnyOrBuilder any) {
            String typeUrl = any.getTypeUrl();
            // Extract the full name from typeUrl (format: "type.googleapis.com/full.name")
            String fullName = typeUrl.substring(typeUrl.lastIndexOf('/') + 1);
            var clz = mustGetMessageClass(fullName);

            Message msg;
            try {
                msg = newBuilder(clz).mergeFrom(any.getValue()).build();
            } catch (Exception e) {
                throw new Json.WriteException("Failed to serialize Any type: " + typeUrl, e);
            }
            var fields = new LinkedHashMap<String, Json.JsonValue>();
            fields.put("@type", new Json.JsonString(typeUrl));
            var jsonValue = messageToJsonValue(msg);
            if (jsonValue instanceof Json.JsonObject obj) {
                fields.putAll(obj.value());
            } else {
                fields.put("value", jsonValue);
            }
            return new Json.JsonObject(fields);
        } else {
            throw new Json.WriteException("Unsupported well-known protobuf type: "
                    + message.getClass().getName());
        }
    }

    static Class<?> mustGetMessageClass(String fullName) {
        Class<?> clz = getWellKnownClass(fullName);
        if (clz == null) {
            clz = getTypeRegistry().get(fullName);
            if (clz == null)
                throw new Json.WriteException("Type not found in registry: " + fullName
                        + ". Make sure the message type is on the classpath.");
        }
        return clz;
    }

    static Json.JsonValue messageToJsonValue(MessageOrBuilder message) {
        if (isWellKnown(message.getDescriptorForType().getFullName())) {
            return wellKnownToJsonValue(message);
        }

        var descriptor = message.getDescriptorForType();
        // For regular messages, serialize all fields to a JSON object
        Map<String, Json.JsonValue> fields =
                new LinkedHashMap<>(mapCap(descriptor.getFields().size()));
        for (var field : descriptor.getFields()) {
            if (unsetOptionalField(message, field)) {
                continue;
            }
            var v = invokeGetter(message, field);
            fields.put(field.getJsonName(), fieldToJsonValue(v, field));
        }
        return new Json.JsonObject(fields);
    }

    static Json.JsonValue fieldToJsonValue(Object value, Descriptors.FieldDescriptor field) {
        if (field.isMapField()) {
            var m = (Map<?, ?>) value;
            Map<String, Json.JsonValue> map = new LinkedHashMap<>(mapCap(m.size()));
            for (var entry : m.entrySet()) {
                Json.JsonValue key = convertSingleValue(
                        entry.getKey(), field.getMessageType().findFieldByNumber(1));
                Json.JsonValue val = convertSingleValue(
                        entry.getValue(), field.getMessageType().findFieldByNumber(2));
                map.put(Json.toString(key), val);
            }
            return new Json.JsonObject(map);
        } else if (field.isRepeated()) {
            List<?> l = (List<?>) value;
            List<Json.JsonValue> list = new ArrayList<>(l.size());
            for (Object item : l) {
                list.add(convertSingleValue(item, field));
            }
            return new Json.JsonArray(list);
        } else {
            return convertSingleValue(value, field);
        }
    }

    static Json.JsonValue convertSingleValue(Object value, Descriptors.FieldDescriptor field) {
        return switch (field.getJavaType()) {
            case INT -> new Json.JsonNumber((Integer) value);
            case LONG -> new Json.JsonNumber((Long) value);
            case FLOAT -> new Json.JsonNumber((Float) value);
            case DOUBLE -> new Json.JsonNumber((Double) value);
            case BOOLEAN -> new Json.JsonBoolean((Boolean) value);
            case STRING -> new Json.JsonString((String) value);
            case BYTE_STRING ->
                new Json.JsonString(Base64.getEncoder().encodeToString(((ByteString) value).toByteArray()));
            case ENUM -> new Json.JsonString(((Enum<?>) value).name());
            case MESSAGE -> messageToJsonValue((MessageOrBuilder) value);
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

    static void mergeIntoBuilder(Json.Parser parser, Json.JsonValue json, Message.Builder builder) {
        if (mergeWellKnown(parser, json, builder)) return;

        // special types here, like Date, DateTime

        Json.JsonObject object = expectObject(json);
        var descriptor = builder.getDescriptorForType();
        var fieldMap = buildFieldMap(descriptor);
        for (var entry : object.value().entrySet()) {
            var field = fieldMap.get(entry.getKey());
            if (field != null) {
                var getterType = getterType(builder, field);
                Object v = parser.parseJsonValue(entry.getValue(), getterType);
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

    static Type getterType(Message.Builder builder, Descriptors.FieldDescriptor field) {
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

    static boolean mergeWellKnown(Json.Parser parser, Json.JsonValue json, Message.Builder builder) {
        if (builder instanceof Timestamp.Builder timestamp) {
            Instant instant = parser.parseJsonValue(json, Instant.class);
            timestamp.setSeconds(instant.getEpochSecond());
            timestamp.setNanos(instant.getNano());
            return true;
        }
        if (builder instanceof com.google.protobuf.Duration.Builder duration) {
            Duration dur = parser.parseJsonValue(json, Duration.class);
            duration.setSeconds(dur.getSeconds());
            duration.setNanos(dur.getNano());
            return true;
        }
        if (builder instanceof StringValue.Builder stringValue) {
            stringValue.setValue(parser.parseJsonValue(json, String.class));
            return true;
        }
        if (builder instanceof BytesValue.Builder bytesValue) {
            byte[] bytes = Base64.getDecoder().decode((String) parser.parseJsonValue(json, String.class));
            bytesValue.setValue(ByteString.copyFrom(bytes));
            return true;
        }
        if (builder instanceof BoolValue.Builder boolValue) {
            boolValue.setValue(parser.parseJsonValue(json, Boolean.class));
            return true;
        }
        if (builder instanceof DoubleValue.Builder doubleValue) {
            doubleValue.setValue(parser.parseJsonValue(json, Double.class));
            return true;
        }
        if (builder instanceof FloatValue.Builder floatValue) {
            floatValue.setValue(parser.parseJsonValue(json, Float.class));
            return true;
        }
        if (builder instanceof Int32Value.Builder int32Value) {
            int32Value.setValue(parser.parseJsonValue(json, Integer.class));
            return true;
        }
        if (builder instanceof UInt32Value.Builder uint32Value) {
            uint32Value.setValue(parser.parseJsonValue(json, Integer.class));
            return true;
        }
        if (builder instanceof Int64Value.Builder int64Value) {
            int64Value.setValue(parser.parseJsonValue(json, Long.class));
            return true;
        }
        if (builder instanceof UInt64Value.Builder uint64Value) {
            uint64Value.setValue(parser.parseJsonValue(json, Long.class));
            return true;
        }
        if (builder instanceof FieldMask.Builder fieldMask) {
            String str = parser.parseJsonValue(json, String.class);
            for (var s : str.split(",")) fieldMask.addPaths(s);
            return true;
        }
        if (builder instanceof Struct.Builder struct) {
            for (var entry : expectObject(json).value().entrySet()) {
                struct.putFields(entry.getKey(), toValue(parser, entry.getValue()));
            }
            return true;
        }
        if (builder instanceof ListValue.Builder list) {
            var ja = json instanceof Json.JsonArray ? (Json.JsonArray) json : new Json.JsonArray(List.of(json));
            for (var v : ja.value()) list.addValues(toValue(parser, v));
            return true;
        }
        if (builder instanceof Value.Builder value) {
            value.mergeFrom(toValue(parser, json));
            return true;
        }
        if (builder instanceof Empty.Builder) {
            return true;
        }
        if (builder instanceof Any.Builder any) {
            parseAny(parser, expectObject(json), any);
            return true;
        }
        return false;
    }

    static void parseAny(Json.Parser parser, Json.JsonObject jo, Any.Builder anyBuilder) {
        Json.JsonValue typeValue = jo.value().get("@type");
        if (typeValue == null) throw new Json.ConversionException("Any type must have @type field");
        if (!(typeValue instanceof Json.JsonString typeStr))
            throw new Json.ConversionException("@type field must be a string");

        String typeUrl = typeStr.value();
        // Extract the full name from typeUrl (format: "type.googleapis.com/full.name")
        String fullName = typeUrl.substring(typeUrl.lastIndexOf('/') + 1);
        Class<?> messageClass = mustGetMessageClass(fullName);

        try {
            // Create a JSON object with all fields except @type
            Map<String, Json.JsonValue> fields = new LinkedHashMap<>();
            for (var entry : jo.value().entrySet()) {
                if (!entry.getKey().equals("@type")) {
                    fields.put(entry.getKey(), entry.getValue());
                }
            }

            // If it's a well-known type with only a "value" field, use it directly
            Json.JsonValue valueToDeserialize;
            if (isWellKnown(fullName) && fields.size() == 1 && fields.containsKey("value")) {
                valueToDeserialize = fields.get("value");
            } else {
                valueToDeserialize = new Json.JsonObject(fields);
            }

            // Parse the nested message using the actual Message class
            Message.Builder builder = newBuilder(messageClass);
            mergeIntoBuilder(parser, valueToDeserialize, builder);
            Message nestedMessage = builder.build();

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

    static Object parseSpecialType(Json.Parser parser, Json.JsonValue jv, Class<?> raw) {
        if (typeBetween(raw, LazyStringArrayList.class, ProtocolStringList.class)) {
            List<String> list = parser.parseJsonValue(jv, new Json.Type<List<String>>() {}.getType());
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

    static Value toValue(Json.Parser parser, Json.JsonValue value) {
        var builder = Value.newBuilder();
        if (value instanceof Json.JsonNull) builder.setNullValue(NullValue.NULL_VALUE);
        else if (value instanceof Json.JsonBoolean b) builder.setBoolValue(b.value());
        else if (value instanceof Json.JsonNumber n)
            builder.setNumberValue(n.value().doubleValue());
        else if (value instanceof Json.JsonString s) builder.setStringValue(s.value());
        else if (value instanceof Json.JsonObject o) {
            var sb = Struct.newBuilder();
            mergeIntoBuilder(parser, o, sb);
            builder.setStructValue(sb.build());
        } else if (value instanceof Json.JsonArray a) {
            var lb = ListValue.newBuilder();
            mergeIntoBuilder(parser, a, lb);
            builder.setListValue(lb.build());
        }
        return builder.build();
    }

    private static class TypeRegistryHolder {
        /**
         * fullName -> Class
         */
        private static final Map<String, Class<?>> INSTANCE = initializeRegistry();

        private static Map<String, Class<?>> initializeRegistry() {
            String cp = System.getProperty("java.class.path");
            if (cp == null || cp.isBlank()) return Map.of();
            Map<String, Class<?>> registry = new ConcurrentHashMap<>();
            for (String e : cp.split(File.pathSeparator)) {
                scanClasspath(e, registry);
            }
            return registry;
        }

        static void scanClasspath(String cp, Map<String, Class<?>> registry) {
            var file = new File(cp);
            if (!file.exists()) return;
            if (file.isFile() && file.getName().endsWith(".jar")) {
                scanJar(file, registry);
            } else if (file.isDirectory()) {
                scanDir(file, registry);
            } else if (file.isFile() && file.getName().endsWith(".class")) {
                try (var is = new FileInputStream(file)) {
                    Clazz clazz = ClassReader.read(is);
                    registerMessageClass(clazz, registry);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        static void scanJar(File jar, Map<String, Class<?>> registry) {
            try (var jarFile = new JarFile(jar)) {
                var entries = jarFile.entries();
                while (entries.hasMoreElements()) {
                    var entry = entries.nextElement();
                    var name = entry.getName();
                    if (name.endsWith(".class")) {
                        Clazz clazz;
                        try (var is = jarFile.getInputStream(entry)) {
                            clazz = ClassReader.read(is);
                        }
                        registerMessageClass(clazz, registry);
                    }
                }
            } catch (Exception ignore) {
            }
        }

        static void scanDir(File dir, Map<String, Class<?>> registry) {
            if (!dir.exists() || !dir.isDirectory()) return;
            var files = dir.listFiles();
            if (files == null) return;
            try {
                for (var file : files) {
                    if (file.isDirectory()) scanDir(file, registry);
                    else if (file.getName().endsWith(".class")) {
                        Clazz clazz;
                        try (var is = new FileInputStream(file)) {
                            clazz = ClassReader.read(is);
                        }
                        registerMessageClass(clazz, registry);
                    }
                }
            } catch (Exception ignored) {
            }
        }

        static void registerMessageClass(Clazz clazz, Map<String, Class<?>> registry) {
            if (clazz != null) {
                if (clazz.supperClass().isPresent()
                        && Modifier.isPublic(clazz.accessFlags())
                        && Modifier.isFinal(clazz.accessFlags())
                        && !Modifier.isInterface(clazz.accessFlags())
                        && !Modifier.isAbstract(clazz.accessFlags())) {
                    var supperClass = clazz.supperClass().get();
                    if (supperClass.equals("com.google.protobuf.GeneratedMessage")
                            || supperClass.equals("com.google.protobuf.GeneratedMessageV3")) {
                        if (clazz.interfaces().stream().anyMatch(e -> e.endsWith("OrBuilder"))) {
                            try {
                                Class<?> clz = Class.forName(clazz.fqn());
                                if (Message.class.isAssignableFrom(clz)) {
                                    Method getDescriptor = clz.getMethod("getDescriptor");
                                    Descriptors.Descriptor descriptor =
                                            (Descriptors.Descriptor) getDescriptor.invoke(null);
                                    registry.put(descriptor.getFullName(), clz);
                                }
                            } catch (Throwable ignored) {
                            }
                        }
                    }
                }
            }
        }
    }

    record Clazz(String fqn, Optional<String> supperClass, List<String> interfaces, int accessFlags) {}

    static final class ClassReader {

        private record ConstantPoolEntry(int tag, Object value) {}

        static final int CONSTANT_Utf8 = 1;
        static final int CONSTANT_Unicode = 2;
        static final int CONSTANT_Integer = 3;
        static final int CONSTANT_Float = 4;
        static final int CONSTANT_Long = 5;
        static final int CONSTANT_Double = 6;
        static final int CONSTANT_Class = 7;
        static final int CONSTANT_String = 8;
        static final int CONSTANT_Fieldref = 9;
        static final int CONSTANT_Methodref = 10;
        static final int CONSTANT_InterfaceMethodref = 11;
        static final int CONSTANT_NameandType = 12;
        static final int CONSTANT_MethodHandle = 15;
        static final int CONSTANT_MethodType = 16;
        static final int CONSTANT_Dynamic = 17;
        static final int CONSTANT_InvokeDynamic = 18;
        static final int CONSTANT_Module = 19;
        static final int CONSTANT_Package = 20;

        public static Clazz read(InputStream is) {
            var dis = is instanceof DataInputStream d ? d : new DataInputStream(is);
            try {
                // 1. magic number (0xCAFEBABE)
                int magic = dis.readInt();
                if (magic != 0xCAFEBABE) {
                    throw new IllegalStateException("Invalid class file format");
                }

                // 2. version
                int minorVersion = dis.readUnsignedShort();
                int majorVersion = dis.readUnsignedShort();

                // 3. constant pool
                int constantPoolCount = dis.readUnsignedShort();
                ConstantPoolEntry[] constantPool = readConstantPool(dis, constantPoolCount);

                // 4. access flags
                int accessFlags = dis.readUnsignedShort();

                // 5. this class
                int thisClass = dis.readUnsignedShort();
                String fqn = resolveClassName(constantPool, thisClass);

                // 6. super class
                int superClass = dis.readUnsignedShort();
                Optional<String> superClassName =
                        superClass == 0 ? Optional.empty() : Optional.of(resolveClassName(constantPool, superClass));

                // 7. interfaces
                var interfaces = new ArrayList<String>();
                int interfacesCount = dis.readUnsignedShort();
                for (int i = 0; i < interfacesCount; i++) {
                    int interfaceIndex = dis.readUnsignedShort();
                    interfaces.add(resolveClassName(constantPool, interfaceIndex));
                }

                return new Clazz(fqn, superClassName, interfaces, accessFlags);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to read class from input stream", e);
            }
        }

        private static String resolveClassName(ConstantPoolEntry[] constantPool, int index) {
            ConstantPoolEntry classInfo = constantPool[index];
            if (classInfo == null || classInfo.tag != CONSTANT_Class) {
                throw new IllegalStateException("Invalid class reference at index " + index);
            }
            int nameIndex = (Integer) classInfo.value;
            ConstantPoolEntry utf8Info = constantPool[nameIndex];
            if (utf8Info == null || utf8Info.tag != CONSTANT_Utf8) {
                throw new IllegalStateException("Invalid UTF8 reference at index " + nameIndex);
            }
            return ((String) utf8Info.value).replace('/', '.');
        }

        private static ConstantPoolEntry[] readConstantPool(DataInputStream dis, int constantPoolCount)
                throws IOException {
            ConstantPoolEntry[] pool = new ConstantPoolEntry[constantPoolCount];

            for (int i = 1; i < constantPoolCount; i++) {
                int tag = dis.readUnsignedByte();

                switch (tag) {
                    case CONSTANT_Utf8 -> {
                        String utf8Value = dis.readUTF();
                        pool[i] = new ConstantPoolEntry(tag, utf8Value);
                    }
                    case CONSTANT_Integer -> {
                        int intValue = dis.readInt();
                        pool[i] = new ConstantPoolEntry(tag, intValue);
                    }
                    case CONSTANT_Float -> {
                        float floatValue = dis.readFloat();
                        pool[i] = new ConstantPoolEntry(tag, floatValue);
                    }
                    case CONSTANT_Long -> {
                        long longValue = dis.readLong();
                        pool[i] = new ConstantPoolEntry(tag, longValue);
                        i++; // Long 占用两个槽位
                    }
                    case CONSTANT_Double -> {
                        double doubleValue = dis.readDouble();
                        pool[i] = new ConstantPoolEntry(tag, doubleValue);
                        i++; // Double 占用两个槽位
                    }
                    case CONSTANT_Class -> {
                        int nameIndex = dis.readUnsignedShort();
                        pool[i] = new ConstantPoolEntry(tag, nameIndex);
                    }
                    case CONSTANT_String -> {
                        int stringIndex = dis.readUnsignedShort();
                        pool[i] = new ConstantPoolEntry(tag, stringIndex);
                    } // 9
                    // 10
                    case CONSTANT_Fieldref, CONSTANT_Methodref, CONSTANT_InterfaceMethodref -> {
                        int classIndex = dis.readUnsignedShort();
                        int nameAndTypeIndex = dis.readUnsignedShort();
                        pool[i] = new ConstantPoolEntry(tag, new int[] {classIndex, nameAndTypeIndex});
                    }
                    case CONSTANT_NameandType -> {
                        int nameIdx = dis.readUnsignedShort();
                        int descriptorIdx = dis.readUnsignedShort();
                        pool[i] = new ConstantPoolEntry(tag, new int[] {nameIdx, descriptorIdx});
                    }
                    case CONSTANT_MethodHandle -> {
                        int referenceKind = dis.readUnsignedByte();
                        int referenceIndex = dis.readUnsignedShort();
                        pool[i] = new ConstantPoolEntry(tag, new int[] {referenceKind, referenceIndex});
                    }
                    case CONSTANT_MethodType -> {
                        int descriptorIndex = dis.readUnsignedShort();
                        pool[i] = new ConstantPoolEntry(tag, descriptorIndex);
                    } // 17
                    case CONSTANT_Dynamic, CONSTANT_InvokeDynamic -> {
                        int bootstrapMethodAttrIndex = dis.readUnsignedShort();
                        int nameAndTypeIdx = dis.readUnsignedShort();
                        pool[i] = new ConstantPoolEntry(tag, new int[] {bootstrapMethodAttrIndex, nameAndTypeIdx});
                    } // 19
                    case CONSTANT_Module, CONSTANT_Package -> {
                        int moduleNameIndex = dis.readUnsignedShort();
                        pool[i] = new ConstantPoolEntry(tag, moduleNameIndex);
                    }
                    default -> throw new IllegalStateException("Unknown constant pool tag: " + tag);
                }
            }

            return pool;
        }
    }
}
