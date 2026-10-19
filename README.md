# analytics-operator-lib

VAR | Default | Desc
------------- | ------------- | -------------
CONFIG_APPLICATION_ID  | stream-operator | Application ID
CONFIG_BOOTSTRAP_SERVERS  | Queried from zookeeper | List of kafka brokers, optional if ZK_Quorum is given
ZK_QUORUM  | localhost:2181 | zookeeper instances
KAFKA_TOPIC  | input-stream | inputstream name
KAFKA_OUTPUT  | output-stream | outputstream name
DEVICE_ID_PATH  | null | The path to device_id value, if nothing is given, the operator will process everything
DEVICE_ID  | null | The device id of the messages to be processed

## Declaring inputs, outputs and configuration

Optional. An operator may declare its inputs, outputs and config values with types; one that does not works as before, and `configMessage()`, `getConfigValue(name, default)` and `message.output(name, value)` stay as they are.

The declaration is an annotation on a `static final String` constant of the operator class. The constant is the name and is used in the code in place of the string literal:

```java
import org.infai.ses.senergy.exceptions.NoValueException;
import org.infai.ses.senergy.operators.*;
import org.infai.ses.senergy.operators.declaration.ConfigValue;
import org.infai.ses.senergy.operators.declaration.Input;
import org.infai.ses.senergy.operators.declaration.Output;
import org.infai.ses.senergy.operators.declaration.Type;

public class Threshold extends BaseOperator {

    @Input(type = Type.FLOAT)
    static final String VALUE = "value";

    @Output(type = Type.BOOL)
    static final String EXCEEDED = "exceeded";

    @ConfigValue(type = Type.FLOAT, defaultValue = "0.5")
    static final String LIMIT = "limit";

    @Override
    public void run(Message message) {
        double limit = Double.parseDouble(getConfig().getConfigValue(LIMIT, "0.5"));
        try {
            message.output(EXCEEDED, message.getInput(VALUE).getValue() > limit);
        } catch (NoValueException e) {
            // no value, no output
        }
    }

    @Override
    public Message configMessage(Message message) {
        message.addInput(VALUE);
        return message;
    }
}
```

- `Type` is one of `STRING`, `FLOAT`, `INT`, `BOOL`; the manifest names them `string`, `float`, `int`, `bool`.
- `@Input` and `@Output` name an annotation of the same name as the class `org.infai.ses.senergy.operators.Input`. Import the annotation by its full name, as above; with `org.infai.ses.senergy.operators.declaration.*` the name `Input` is ambiguous.
- An annotation on anything but a `static final String` constant is an error. A name declared twice in one section is an error.
- `defaultValue` documents the default. `getConfigValue` still returns the default the operator passes it, and the manifest lists only name and type.
- Declared superclasses come first, then the declaration order of the source.
- At runtime the library warns once per name when the operator declares outputs and `message.output` is called with a name that none of them has. An operator without `@Output` declarations is not checked.

## Manifest

The platform registers an operator version from `operator-manifest.json`. The generator writes it from the annotations:

```
java -cp <classpath> org.infai.ses.senergy.operators.Manifest <fully qualified operator class> [-o operator-manifest.json]
```

The classpath has to contain the operator and this library. After `mvn package`, an operator that builds a jar with dependencies can run it from that jar:

```
java -cp target/operator-<name>-jar-with-dependencies.jar org.infai.ses.senergy.operators.Manifest com.example.Threshold
```

The output has the `inputs`, `outputs` and `config_values` of the operator, each a list of `{"name", "type"}`. `original_input_ids`, which the library adds to every operator and flows wire, ends the inputs and the outputs without a declaration:

```json
{
  "inputs": [
    {
      "name": "value",
      "type": "float"
    },
    {
      "name": "original_input_ids",
      "type": "string"
    }
  ],
  "outputs": [
    {
      "name": "exceeded",
      "type": "bool"
    },
    {
      "name": "original_input_ids",
      "type": "string"
    }
  ],
  "config_values": [
    {
      "name": "limit",
      "type": "float"
    }
  ]
}
```

Without at least one `@Input` and one `@Output` it writes nothing, says so and exits 0. Any other problem (class not found, not an operator, a wrongly declared field, an unwritable target) exits 1 with the reason on stderr. The operator is not constructed and runs nothing. The format is the one of the manifest in the Python operator library.
