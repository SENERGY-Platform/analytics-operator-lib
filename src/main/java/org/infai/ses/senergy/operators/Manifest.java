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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.infai.ses.senergy.exceptions.DeclarationException;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes operator-manifest.json from the declarations of an operator class:
 *
 * <pre>
 *     java -cp &lt;classpath&gt; org.infai.ses.senergy.operators.Manifest &lt;operator class&gt; [-o operator-manifest.json]
 * </pre>
 *
 * Same content and format as the manifest of the Python operator library.
 */
public final class Manifest {

    private static final String DEFAULT_OUTPUT = "operator-manifest.json";
    private static final String USAGE = "usage: Manifest <operator class> [-o " + DEFAULT_OUTPUT + "]";

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();

    private Manifest() {
        throw new IllegalStateException("Utility class");
    }

    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err));
    }

    static int run(String[] args, PrintStream out, PrintStream err) {
        String className = null;
        String output = DEFAULT_OUTPUT;
        for (int i = 0; i < args.length; i++) {
            if (("-o".equals(args[i]) || "--output".equals(args[i])) && i + 1 < args.length) {
                output = args[++i];
            } else if (className == null && !args[i].startsWith("-")) {
                className = args[i];
            } else {
                err.println("error: " + USAGE);
                return 1;
            }
        }
        if (className == null) {
            err.println("error: " + USAGE);
            return 1;
        }
        try {
            // Not initialized here: the constants are read through reflection, and no operator is constructed.
            Class<?> operator = Class.forName(className, false, Manifest.class.getClassLoader());
            if (!OperatorInterface.class.isAssignableFrom(operator)) {
                throw new DeclarationException(className + " is not an operator, it does not implement OperatorInterface");
            }
            Map<String, List<Map<String, String>>> manifest = build(Declarations.of(operator));
            if (manifest == null) {
                out.println(operator.getSimpleName() + " declares no inputs and outputs, nothing written");
                return 0;
            }
            Files.writeString(Path.of(output), GSON.toJson(manifest) + "\n");
            out.println("wrote " + output);
            return 0;
        } catch (ClassNotFoundException e) {
            err.println("error: class " + className + " not found on the classpath");
        } catch (DeclarationException e) {
            err.println("error: " + e.getMessage());
        } catch (IOException e) {
            err.println("error: cannot write " + output + ": " + e);
        } catch (LinkageError e) {
            err.println("error: cannot load " + className + ": " + e);
        }
        return 1;
    }

    /**
     * The manifest as a map in the order of the JSON, or null unless the operator
     * declares at least one input and one output.
     */
    static Map<String, List<Map<String, String>>> build(Declarations declarations) {
        if (declarations.inputs().isEmpty() || declarations.outputs().isEmpty()) {
            return null;
        }
        Map<String, List<Map<String, String>>> manifest = new LinkedHashMap<>();
        manifest.put("inputs", entries(declarations.inputs()));
        manifest.put("outputs", entries(declarations.outputs()));
        manifest.put("config_values", entries(declarations.configValues()));
        return manifest;
    }

    private static List<Map<String, String>> entries(List<Declarations.Port> ports) {
        List<Map<String, String>> entries = new ArrayList<>();
        for (Declarations.Port port : ports) {
            Map<String, String> entry = new LinkedHashMap<>();
            entry.put("name", port.name());
            entry.put("type", port.type().manifestName());
            entries.add(entry);
        }
        return entries;
    }
}
