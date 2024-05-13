# EMQX Learning

## Performance test

+ To start benchmark test using `mqtt-publisher`, access the interactive shell of `mqtt-publisher` container, then execute below command:

```sh
./EmqxLearning.MqttPublisher -n=15000 -I=980 -q=1 -m='{"messageId":1,"temperature":32.81665161616013,"humidity":71.98951628617453,"deviceId":"DB","timestamp":1679898067325,"ack":true,"snr":9,"txt":"text"}'
```

Large payload ~4KB (remove `-m` option)

```sh
./EmqxLearning.MqttPublisher -n=3000 -I=980 -q=1
```

+ To start benchmark test using `emqtt-bench`, access the interactive shell of `emqtt-bench` container, then execute below command:

```sh
./custom_eqmtt_bench.sh 10 -c 100 -I 100 -q 1 -m '{"messageId":1,"temperature":32.81665161616013,"humidity":71.98951628617453,"deviceId":"DB","timestamp":1679898067325,"ack":true,"snr":9,"txt":"text"}'
```

## Utilities

+ To check number of connections

```sql
select count(*)
from pg_stat_activity
where datname = 'device'
and query like '%INSERT INTO device_metric_series%';
```

+ To disconnect container from network

```sh
docker network disconnect emqx-learning_emqx-bridge {container-id}
```

+ To connect container to network

```sh
docker network connect --alias {alias} emqx-learning_emqx-bridge {container-id}
```
