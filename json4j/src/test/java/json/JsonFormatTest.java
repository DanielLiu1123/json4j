package json;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import com.google.protobuf.Any;
import com.google.protobuf.Int32Value;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class JsonFormatTest {

    @Test
    void print() throws Exception {
        var types = List.of(Timestamp.getDescriptor(), StringValue.getDescriptor(), Int32Value.getDescriptor());

        var printer = JsonFormat.printer()
                .usingTypeRegistry(
                        JsonFormat.TypeRegistry.newBuilder().add(types).build())
                .omittingInsignificantWhitespace()
                .includingDefaultValueFields();

        // @spotless:off
        var table = new Object[][]{
                {Timestamps.parse("2025-10-19T03:29:19.500Z"), "\"2025-10-19T03:29:19.500Z\""},
                {Any.pack(Timestamps.parse("2025-10-19T03:29:19.500Z")), "{\"@type\":\"type.googleapis.com/google.protobuf.Timestamp\",\"value\":\"2025-10-19T03:29:19.500Z\"}"},
                {StringValue.of("hello"), "\"hello\""},
                {Any.pack(StringValue.of("hello")), "{\"@type\":\"type.googleapis.com/google.protobuf.StringValue\",\"value\":\"hello\"}"},
                {Int32Value.of(1), "1"},
                {Any.pack(Int32Value.of(1)), "{\"@type\":\"type.googleapis.com/google.protobuf.Int32Value\",\"value\":1}"},
        };
        // @spotless:on

        assertAll(IntStream.range(0, table.length).mapToObj(i -> () -> {
            var row = table[i];
            var input = (MessageOrBuilder) row[0];
            var expected = (String) row[1];
            var actual = printer.print(input);
            assertThat(actual).as("Case %d: input=%s", i, input).isEqualTo(expected);
        }));
    }
}
