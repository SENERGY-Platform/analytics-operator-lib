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

import org.infai.ses.senergy.operators.declaration.ConfigValue;
import org.infai.ses.senergy.operators.declaration.Input;
import org.infai.ses.senergy.operators.declaration.Output;
import org.infai.ses.senergy.operators.declaration.Type;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ManifestTest {

    // Declared in an order that is neither alphabetical nor by type, so a sorted
    // manifest would differ from the source.
    static class Parent extends BaseOperator {
        @Input(type = Type.STRING)
        static final String PARENT_INPUT = "mode";
    }

    static class Declared extends Parent {
        @Input(type = Type.FLOAT)
        static final String ZETA = "zeta";
        @Input(type = Type.BOOL)
        static final String ALPHA = "alpha";
        @Output(type = Type.INT)
        static final String SECOND = "second";
        @Output(type = Type.STRING)
        private static final String FIRST = "first";
        @ConfigValue(type = Type.FLOAT, defaultValue = "0.5")
        static final String THRESHOLD = "threshold";
        @ConfigValue(type = Type.BOOL, defaultValue = "")
        static final String ENABLED = "enabled";
        // Not a declaration: no annotation.
        static final String UNRELATED = "unrelated";
    }

    static class NoConfig extends BaseOperator {
        @Input(type = Type.FLOAT)
        static final String VALUE = "value";
        @Output(type = Type.FLOAT)
        static final String RESULT = "result";
    }

    static class OnlyInputs extends BaseOperator {
        @Input(type = Type.FLOAT)
        static final String VALUE = "value";
        @ConfigValue(type = Type.INT, defaultValue = "1")
        static final String LIMIT = "limit";
    }

    static class OnlyOutputs extends BaseOperator {
        @Output(type = Type.FLOAT)
        static final String RESULT = "result";
    }

    static class Undeclared extends BaseOperator {
    }

    static class NotFinal extends BaseOperator {
        @Input(type = Type.FLOAT)
        @SuppressWarnings("FieldMayBeFinal")
        static String VALUE = "value";
        @Output(type = Type.FLOAT)
        static final String RESULT = "result";
    }

    static class NotStatic extends BaseOperator {
        @Input(type = Type.FLOAT)
        final String value = "value";
        @Output(type = Type.FLOAT)
        static final String RESULT = "result";
    }

    static class NotString extends BaseOperator {
        @Input(type = Type.FLOAT)
        static final int VALUE = 1;
        @Output(type = Type.FLOAT)
        static final String RESULT = "result";
    }

    static class Twice extends BaseOperator {
        @Input(type = Type.FLOAT)
        static final String VALUE = "value";
        @Input(type = Type.INT)
        static final String VALUE_AGAIN = "value";
        @Output(type = Type.FLOAT)
        static final String RESULT = "result";
    }

    static class NotAnOperator {
        @Input(type = Type.FLOAT)
        static final String VALUE = "value";
        @Output(type = Type.FLOAT)
        static final String RESULT = "result";
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();
    private Path target;

    @Before
    public void setUp() {
        target = folder.getRoot().toPath().resolve("operator-manifest.json");
    }

    private int run(String... args) {
        return Manifest.run(args, new PrintStream(out, true, StandardCharsets.UTF_8), new PrintStream(err, true, StandardCharsets.UTF_8));
    }

    private int generate(Class<?> operator) {
        return run(operator.getName(), "-o", target.toString());
    }

    private String stdout() {
        return out.toString(StandardCharsets.UTF_8);
    }

    private String stderr() {
        return err.toString(StandardCharsets.UTF_8);
    }

    @Test
    public void testManifestKeepsSourceOrderTypesAndLeavesDefaultsOut() throws IOException {
        assertEquals(0, generate(Declared.class));
        // Superclass first, then the order of the source, then original_input_ids; only name and type, two-space indent, final newline.
        String expected = String.join("\n",
                "{",
                "  \"inputs\": [",
                "    {",
                "      \"name\": \"mode\",",
                "      \"type\": \"string\"",
                "    },",
                "    {",
                "      \"name\": \"zeta\",",
                "      \"type\": \"float\"",
                "    },",
                "    {",
                "      \"name\": \"alpha\",",
                "      \"type\": \"bool\"",
                "    },",
                "    {",
                "      \"name\": \"original_input_ids\",",
                "      \"type\": \"string\"",
                "    }",
                "  ],",
                "  \"outputs\": [",
                "    {",
                "      \"name\": \"second\",",
                "      \"type\": \"int\"",
                "    },",
                "    {",
                "      \"name\": \"first\",",
                "      \"type\": \"string\"",
                "    },",
                "    {",
                "      \"name\": \"original_input_ids\",",
                "      \"type\": \"string\"",
                "    }",
                "  ],",
                "  \"config_values\": [",
                "    {",
                "      \"name\": \"threshold\",",
                "      \"type\": \"float\"",
                "    },",
                "    {",
                "      \"name\": \"enabled\",",
                "      \"type\": \"bool\"",
                "    }",
                "  ]",
                "}",
                "");
        assertEquals(expected, Files.readString(target));
        assertEquals("wrote " + target + System.lineSeparator(), stdout());
    }

    @Test
    public void testManifestWithoutConfigValuesHasEmptyList() throws IOException {
        assertEquals(0, generate(NoConfig.class));
        assertTrue(Files.readString(target).contains("\"config_values\": []\n}\n"));
    }

    @Test
    public void testNoOutputFileWithoutInputsAndOutputs() {
        for (Class<?> operator : List.of(Undeclared.class, OnlyInputs.class, OnlyOutputs.class)) {
            out.reset();
            assertEquals(operator.getSimpleName(), 0, generate(operator));
            assertFalse(operator.getSimpleName(), Files.exists(target));
            assertEquals(operator.getSimpleName() + " declares no inputs and outputs, nothing written" + System.lineSeparator(), stdout());
            assertEquals("", stderr());
        }
    }

    @Test
    public void testExistingFileStaysWhenNothingIsWritten() throws IOException {
        Files.writeString(target, "old");
        assertEquals(0, generate(Undeclared.class));
        assertEquals("old", Files.readString(target));
    }

    @Test
    public void testFieldThatIsNotAStaticFinalStringFails() {
        for (Class<?> operator : List.of(NotFinal.class, NotStatic.class, NotString.class)) {
            err.reset();
            assertEquals(operator.getSimpleName(), 1, generate(operator));
            assertFalse(operator.getSimpleName(), Files.exists(target));
            assertTrue(stderr(), stderr().startsWith("error: "));
            assertTrue(stderr(), stderr().contains(operator.getName() + ".") && stderr().contains("static final String"));
        }
    }

    @Test
    public void testNameDeclaredTwiceFails() {
        assertEquals(1, generate(Twice.class));
        assertTrue(stderr(), stderr().contains("\"value\" is declared twice"));
        assertFalse(Files.exists(target));
    }

    @Test
    public void testUnknownClassAndNonOperatorAndUsageFail() {
        assertEquals(1, run("no.such.Operator", "-o", target.toString()));
        assertTrue(stderr(), stderr().contains("no.such.Operator not found on the classpath"));

        err.reset();
        assertEquals(1, generate(NotAnOperator.class));
        assertTrue(stderr(), stderr().contains("is not an operator"));

        err.reset();
        assertEquals(1, run());
        assertTrue(stderr(), stderr().contains("usage:"));

        err.reset();
        assertEquals(1, run(Declared.class.getName(), "-o"));
        assertTrue(stderr(), stderr().contains("usage:"));
        assertFalse(Files.exists(target));
    }

    @Test
    public void testUnwritableTargetFails() {
        assertEquals(1, run(Declared.class.getName(), "-o", folder.getRoot().toPath().resolve("missing/dir/m.json").toString()));
        assertTrue(stderr(), stderr().startsWith("error: cannot write "));
    }

    // Undeclared outputs

    private static class Records extends Handler {
        final List<String> messages = new ArrayList<>();

        @Override
        public void publish(LogRecord record) {
            messages.add(java.text.MessageFormat.format(record.getMessage(), record.getParameters()));
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void testUndeclaredOutputIsWarnedOncePerName() throws Exception {
        Logger logger = Logger.getLogger(Declarations.class.getName());
        Records records = new Records();
        logger.addHandler(records);
        try {
            Declarations declarations = Declarations.of(NoConfig.class);
            declarations.checkOutput("result");
            declarations.checkOutput("extra");
            declarations.checkOutput("extra");
            declarations.checkOutput("other");
            declarations.checkOutput(Declarations.ORIGINAL_INPUT_IDS);
            assertEquals(List.of(
                    "Output extra is not declared by " + NoConfig.class.getName() + ".",
                    "Output other is not declared by " + NoConfig.class.getName() + "."), records.messages);
        } finally {
            logger.removeHandler(records);
        }
    }

    @Test
    public void testNoWarningWithoutDeclaredOutputs() throws Exception {
        Logger logger = Logger.getLogger(Declarations.class.getName());
        Records records = new Records();
        logger.addHandler(records);
        try {
            Declarations.of(Undeclared.class).checkOutput("anything");
            Declarations.of(OnlyInputs.class).checkOutput("anything");
            assertEquals(List.of(), records.messages);
        } finally {
            logger.removeHandler(records);
        }
    }
}
