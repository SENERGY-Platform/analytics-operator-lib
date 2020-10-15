/*
 * Copyright 2020 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.infai.ses.senergy.utils;

import java.time.*;
import java.time.format.DateTimeFormatter;

public class TimeProvider {

    private static Clock clock = Clock.systemDefaultZone();
    private static ZoneId zoneId = ZoneId.systemDefault();

    private TimeProvider() {
        throw new IllegalStateException("Utility class");
    }

    public static LocalDateTime now() {
        return LocalDateTime.now(clock);
    }

    public static String nowUTCToString() {
        DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;
        return ZonedDateTime.of(LocalDateTime.now(clock), zoneId).format(formatter);
    }

    public static LocalDateTime utc(){
        return OffsetDateTime.now(ZoneOffset.ofHours(0)).toLocalDateTime();
    }

    public static void useFixedClockAt(LocalDateTime date) {
        clock = Clock.fixed(date.atZone(zoneId).toInstant(), zoneId);
    }
}
