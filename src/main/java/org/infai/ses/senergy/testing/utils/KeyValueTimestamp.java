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
 *
 */

package org.infai.ses.senergy.testing.utils;

public class KeyValueTimestamp<K, V> {
    public final K key;
    public final V value;
    public final long timestamp;

    public KeyValueTimestamp(K key, V value, long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KeyValueTimestamp)) return false;
        KeyValueTimestamp<?, ?> that = (KeyValueTimestamp<?, ?>) o;
        return timestamp == that.timestamp &&
                java.util.Objects.equals(key, that.key) &&
                java.util.Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(key, value, timestamp);
    }

    @Override
    public String toString() {
        return "KeyValueTimestamp{" +
                "key=" + key +
                ", value=" + value +
                ", timestamp=" + timestamp +
                '}';
    }
}