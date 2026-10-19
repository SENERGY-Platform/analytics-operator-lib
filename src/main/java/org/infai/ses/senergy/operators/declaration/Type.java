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

package org.infai.ses.senergy.operators.declaration;

/**
 * The type of a declared input, output or config value, as the manifest names it.
 */
public enum Type {
    STRING("string"),
    FLOAT("float"),
    INT("int"),
    BOOL("bool");

    private final String manifestName;

    Type(String manifestName) {
        this.manifestName = manifestName;
    }

    public String manifestName() {
        return manifestName;
    }
}
