package json;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

/**
 *
 *
 * @author Freeman
 * @since 2025/10/10
 */
class JacksonTest {

    @Test
    void testDate() {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        var jsonMapper = JsonMapper.builder().build();
        System.out.println("Date: " + jsonMapper.writeValueAsString(new Date()));
        System.out.println("LocalDate: " + jsonMapper.writeValueAsString(LocalDate.now()));
        System.out.println("LocalDateTime: " + jsonMapper.writeValueAsString(LocalDateTime.now()));
        System.out.println("Instant: " + jsonMapper.writeValueAsString(Instant.now()));
        System.out.println("LocalTime: " + jsonMapper.writeValueAsString(LocalTime.now()));
        System.out.println("Duration: "
                + jsonMapper.writeValueAsString(
                        Duration.ofHours(25).plus(Duration.ofMinutes(30)).plus(Duration.ofDays(1))));
    }
}
