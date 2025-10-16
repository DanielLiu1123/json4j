package json;

/**
 * Exception thrown when JSON parsing, serialization, or type conversion fails.
 * This is the base exception for all json4j-related errors.
 *
 * @author Freeman
 * @since 0.3.0
 */
public class JsonException extends RuntimeException {

    /**
     * Constructs a new JsonException with the specified detail message.
     *
     * @param message the detail message
     */
    public JsonException(String message) {
        super(message);
    }

    /**
     * Constructs a new JsonException with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause   the cause
     */
    public JsonException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Exception thrown when JSON parsing fails due to malformed JSON syntax.
     */
    public static class ParseException extends JsonException {
        private final int line;
        private final int column;

        public ParseException(String message, int line, int column) {
            super(String.format("%s at line %d, column %d", message, line, column));
            this.line = line;
            this.column = column;
        }

        public ParseException(String message, int line, int column, Throwable cause) {
            super(String.format("%s at line %d, column %d", message, line, column), cause);
            this.line = line;
            this.column = column;
        }

        public int getLine() {
            return line;
        }

        public int getColumn() {
            return column;
        }
    }

    /**
     * Exception thrown when type conversion fails during deserialization.
     */
    public static class TypeConversionException extends JsonException {
        private final Class<?> targetType;
        private final Object sourceValue;

        public TypeConversionException(String message, Class<?> targetType, Object sourceValue) {
            super(message);
            this.targetType = targetType;
            this.sourceValue = sourceValue;
        }

        public TypeConversionException(String message, Class<?> targetType, Object sourceValue, Throwable cause) {
            super(message, cause);
            this.targetType = targetType;
            this.sourceValue = sourceValue;
        }

        public Class<?> getTargetType() {
            return targetType;
        }

        public Object getSourceValue() {
            return sourceValue;
        }
    }

    /**
     * Exception thrown when bean or record deserialization fails.
     */
    public static class BeanException extends JsonException {
        private final Class<?> beanType;
        private final String propertyName;

        public BeanException(String message, Class<?> beanType, String propertyName) {
            super(String.format("%s (bean: %s, property: %s)", message, beanType.getName(), propertyName));
            this.beanType = beanType;
            this.propertyName = propertyName;
        }

        public BeanException(String message, Class<?> beanType, String propertyName, Throwable cause) {
            super(String.format("%s (bean: %s, property: %s)", message, beanType.getName(), propertyName), cause);
            this.beanType = beanType;
            this.propertyName = propertyName;
        }

        public BeanException(String message, Class<?> beanType) {
            super(String.format("%s (bean: %s)", message, beanType.getName()));
            this.beanType = beanType;
            this.propertyName = null;
        }

        public BeanException(String message, Class<?> beanType, Throwable cause) {
            super(String.format("%s (bean: %s)", message, beanType.getName()), cause);
            this.beanType = beanType;
            this.propertyName = null;
        }

        public Class<?> getBeanType() {
            return beanType;
        }

        public String getPropertyName() {
            return propertyName;
        }
    }

    /**
     * Exception thrown when record deserialization fails.
     */
    public static class RecordException extends JsonException {
        private final Class<?> recordType;
        private final String componentName;

        public RecordException(String message, Class<?> recordType, String componentName) {
            super(String.format("%s (record: %s, component: %s)", message, recordType.getName(), componentName));
            this.recordType = recordType;
            this.componentName = componentName;
        }

        public RecordException(String message, Class<?> recordType, String componentName, Throwable cause) {
            super(String.format("%s (record: %s, component: %s)", message, recordType.getName(), componentName), cause);
            this.recordType = recordType;
            this.componentName = componentName;
        }

        public RecordException(String message, Class<?> recordType) {
            super(String.format("%s (record: %s)", message, recordType.getName()));
            this.recordType = recordType;
            this.componentName = null;
        }

        public RecordException(String message, Class<?> recordType, Throwable cause) {
            super(String.format("%s (record: %s)", message, recordType.getName()), cause);
            this.recordType = recordType;
            this.componentName = null;
        }

        public Class<?> getRecordType() {
            return recordType;
        }

        public String getComponentName() {
            return componentName;
        }
    }
}
