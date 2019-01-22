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