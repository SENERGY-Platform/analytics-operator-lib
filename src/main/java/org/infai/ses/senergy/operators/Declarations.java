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

package org.infai.ses.senergy.operators;

import org.infai.ses.senergy.exceptions.DeclarationException;
import org.infai.ses.senergy.operators.declaration.ConfigValue;
import org.infai.ses.senergy.operators.declaration.Input;
import org.infai.ses.senergy.operators.declaration.Output;
import org.infai.ses.senergy.operators.declaration.Type;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The inputs, outputs and config values an operator class declares with
 * {@link Input}, {@link Output} and {@link ConfigValue}, in the order of the source.
 */
final class Declarations {

    /** Added by the library to every operator; the manifest lists it without a declaration. */
    static final String ORIGINAL_INPUT_IDS = "original_input_ids";

    private static final Logger log = Logger.getLogger(Declarations.class.getName());

    record Port(String name, Type type) {
    }

    private final Class<?> operator;
    private final List<Port> inputs = new ArrayList<>();
    private final List<Port> outputs = new ArrayList<>();
    private final List<Port> configValues = new ArrayList<>();
    private final Set<String> warned = ConcurrentHashMap.newKeySet();

    private Declarations(Class<?> operator) {
        this.operator = operator;
    }

    /**
     * Reads the declarations of the class and its superclasses, superclasses first.
     *
     * @throws DeclarationException if an annotated field is not a usable declaration
     */
    static Declarations of(Class<?> operator) throws DeclarationException {
        Declarations declarations = new Declarations(operator);
        List<Class<?>> hierarchy = new ArrayList<>();
        for (Class<?> c = operator; c != null && c != Object.class; c = c.getSuperclass()) {
            hierarchy.add(0, c);
        }
        for (Class<?> c : hierarchy) {
            for (Field field : c.getDeclaredFields()) {
                declarations.read(c, field);
            }
        }
        return declarations;
    }

    List<Port> inputs() {
        return Collections.unmodifiableList(inputs);
    }

    List<Port> outputs() {
        return Collections.unmodifiableList(outputs);
    }

    List<Port> configValues() {
        return Collections.unmodifiableList(configValues);
    }

    /**
     * Warns once per name when the operator declares outputs and writes one that is
     * not among them. Without declared outputs there is nothing to compare with.
     */
    void checkOutput(String name) {
        if (outputs.isEmpty() || ORIGINAL_INPUT_IDS.equals(name)) {
            return;
        }
        for (Port port : outputs) {
            if (port.name().equals(name)) {
                return;
            }
        }
        if (warned.add(name)) {
            log.log(Level.WARNING, "Output {0} is not declared by {1}.", new Object[]{name, operator.getName()});
        }
    }

    private void read(Class<?> owner, Field field) throws DeclarationException {
        Input input = field.getAnnotation(Input.class);
        Output output = field.getAnnotation(Output.class);
        ConfigValue configValue = field.getAnnotation(ConfigValue.class);
        int count = (input != null ? 1 : 0) + (output != null ? 1 : 0) + (configValue != null ? 1 : 0);
        if (count == 0) {
            return;
        }
        String where = owner.getName() + "." + field.getName();
        if (count > 1) {
            throw new DeclarationException(where + ": a field declares one of @Input, @Output, @ConfigValue, not several");
        }
        int modifiers = field.getModifiers();
        if (!Modifier.isStatic(modifiers) || !Modifier.isFinal(modifiers) || field.getType() != String.class) {
            throw new DeclarationException(where + ": a declaration must be a static final String constant");
        }
        String name;
        try {
            field.setAccessible(true);
            name = (String) field.get(null);
        } catch (ReflectiveOperationException | RuntimeException e) {
            throw new DeclarationException(where + ": cannot read the constant: " + e);
        }
        if (name == null || name.isBlank()) {
            throw new DeclarationException(where + ": the constant is the name and must not be empty");
        }
        if (input != null) {
            add(inputs, where, name, input.type());
        } else if (output != null) {
            add(outputs, where, name, output.type());
        } else {
            add(configValues, where, name, configValue.type());
        }
    }

    private static void add(List<Port> ports, String where, String name, Type type) throws DeclarationException {
        if (ports.stream().anyMatch(p -> p.name().equals(name))) {
            throw new DeclarationException(where + ": \"" + name + "\" is declared twice");
        }
        ports.add(new Port(name, type));
    }
}
