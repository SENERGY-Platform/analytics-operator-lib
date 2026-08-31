/*
 * Copyright 2026 InfAI (CC SES)
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

import com.google.gson.Gson;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * The OpenTelemetry baggage of the request that started this pipeline, handed to
 * the operator by the flow engine in the BAGGAGE environment variable.
 *
 * <p>Its entries become static fields on every log record, so a line from this
 * operator can be traced back to the caller's context — to the smart service
 * instance it belongs to, for instance. The pod labels the flow engine sets carry
 * the same entries for the log aggregation, which only sees the container from the
 * outside.
 *
 * <p>The entries end up in the encoder's customFields rather than in the SLF4J MDC.
 * The MDC is thread-local, and an operator logs from the Kafka Streams threads,
 * which are not the thread that read the environment; a value put into the MDC at
 * startup would be missing from exactly the records that matter. The baggage
 * describes the whole process anyway, which is what customFields is for.
 */
public final class Baggage {

    /** The environment variable the flow engine passes the baggage in. */
    public static final String ENV_VAR = "BAGGAGE";

    /**
     * The system property logback.xml reads the encoder's customFields from. Set
     * before the first logger is created, because logback resolves it while it is
     * reading its configuration.
     */
    public static final String CUSTOM_FIELDS_PROPERTY = "custom.fields";

    public static final String ORGANIZATION = "github.com/SENERGY-Platform";

    /**
     * Keys the log record owns. A baggage entry using one of them would produce a
     * duplicate JSON key and make the field it collides with unreadable, so it is
     * dropped. The first five match the fieldNames in logback.xml.
     */
    private static final Set<String> RESERVED_KEYS =
            Set.of("time", "level", "msg", "logger", "thread", "organization", "project");

    private Baggage() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Parse a W3C baggage header.
     *
     * <pre>
     *     baggage-string = list-member *( OWS "," OWS list-member )
     *     list-member    = key OWS "=" OWS value *( OWS ";" OWS property )
     * </pre>
     *
     * <p>Properties are dropped: nothing in the platform sets them, and they are
     * metadata about an entry rather than context worth logging. A malformed entry
     * is skipped rather than thrown on — the baggage exists so that logs can be
     * correlated, and an operator that refused to start over an unparseable
     * annotation would trade a diagnostic aid for an outage.
     *
     * @param header the header value, may be null or empty
     * @return the entries, in the order they appeared; never null
     */
    public static Map<String, String> parse(String header) {
        Map<String, String> entries = new LinkedHashMap<>();
        if (header == null || header.isEmpty()) {
            return entries;
        }
        for (String member : header.split(",")) {
            int propertyStart = member.indexOf(';');
            if (propertyStart >= 0) {
                member = member.substring(0, propertyStart);
            }
            member = member.trim();
            int separator = member.indexOf('=');
            if (separator <= 0) {
                continue;
            }
            String key = member.substring(0, separator).trim();
            if (key.isEmpty()) {
                continue;
            }
            entries.put(key, percentDecode(member.substring(separator + 1).trim()));
        }
        return entries;
    }

    /**
     * Read the baggage out of the environment.
     */
    public static Map<String, String> fromEnvironment() {
        return parse(System.getenv(ENV_VAR));
    }

    /**
     * Build the JSON for the encoder's customFields: the organization and project
     * this log stream belongs to, plus the baggage.
     */
    public static String customFields(String project, Map<String, String> baggage) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("organization", ORGANIZATION);
        fields.put("project", project == null || project.isEmpty() ? "unknown" : project);
        for (Map.Entry<String, String> entry : baggage.entrySet()) {
            if (RESERVED_KEYS.contains(entry.getKey())) {
                continue;
            }
            fields.put(entry.getKey(), entry.getValue());
        }
        return new Gson().toJson(fields);
    }

    /**
     * Percent-decode a baggage value.
     *
     * <p>Hand-rolled rather than through URLDecoder, which is a form decoder: it
     * turns a plus sign into a space, while in a baggage value a plus sign is a
     * literal one. It also throws on a malformed escape, where here the value is
     * better kept as written than lost.
     */
    private static String percentDecode(String value) {
        if (value.indexOf('%') < 0) {
            return value;
        }
        ByteArrayOutputStream decoded = new ByteArrayOutputStream(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '%' && i + 2 < value.length()) {
                int high = Character.digit(value.charAt(i + 1), 16);
                int low = Character.digit(value.charAt(i + 2), 16);
                if (high >= 0 && low >= 0) {
                    decoded.write((high << 4) + low);
                    i += 2;
                    continue;
                }
            }
            // Not a valid escape: keep the character as it stands. A multi-byte one
            // cannot legally appear unencoded, but writing its own bytes is still the
            // truthful thing to do with it.
            decoded.writeBytes(String.valueOf(c).getBytes(StandardCharsets.UTF_8));
        }
        return decoded.toString(StandardCharsets.UTF_8);
    }
}
