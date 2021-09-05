# Delayed message Example

This example demonstrates how to use delayed message delivery feature.

## Prerequisites

- Docker Desktop

## Examples

- [DelayedAfter Message Producer](../SharpPulsar-Examples/examples/DelayedMessages/DelayedAfterMessageProducer.cs) 

It will publish 5 messages immediately and publish another 5 messages delayed 5 seconds by using `deliverAfter`.

- [DelayedAt Message Producer](../SharpPulsar-Examples/examples/DelayedMessages/DelayedAtMessageProducer.cs)  

It will publish 5 messages immediately and publish another 5 messages delayed 5 seconds by using `deliverAt`.

- [Delayed Message Consumer](../SharpPulsar-Examples/examples/DelayedMessages/DelayedMessageConsumer.cs)

It runs in a loop receiving messages.

- Expected Result

Consumer will receive no delayed messages immediately and receive delayed messages after 5 seconds.

## Steps

1. Start Pulsar standalone
   ```
   docker run --name pulsar_local -it --env PULSAR_MEM="-Xms512m -Xmx512m -XX:MaxDirectMemorySize=1g" --env PULSAR_PREFIX_acknowledgmentAtBatchIndexLevelEnabled=true --env PULSAR_PREFIX_nettyMaxFrameSizeBytes=5253120 --env PULSAR_PREFIX_brokerDeleteInactiveTopicsEnabled=false --env PULSAR_PREFIX_transactionCoordinatorEnabled=true -p 6650:6650 -p 8080:8080 -p 8081:8081 --mount source=pulsarconf,target=/pulsar/conf  apachepulsar/pulsar-all:2.8.0 bash -c "bin/apply-config-from-env.py conf/standalone.conf && bin/pulsar standalone -nfw && bin/pulsar initialize-transaction-coordinator-metadata -cs localhost:2181 -c standalone --initial-num-transaction-coordinators 6 && bin/pulsar-admin namespaces set-retention public/default --time 360000 --size -1"
    
   ```

2. Build the project and cd into the bin directory from cmd/cli

3. Open terminal/cmd/cli, run the consumer example to wait for receiving the produced message from topic
   ```
    SharpPulsar-Examples DelayedMessageConsumer -t public/default/delayed-delivery-example-topic -sn test-sub -st Shared -n 0
   ```

5. Open another terminal, run the DelayedAfterMessageProducer example to produce 10 messages to the topic.
   The producer example will produce the first 5 messages immediately and produce 5 messages delayed 5 seconds using `deliverAfter`.
   ```
    SharpPulsar-Examples delayedaftermessageproducer -t public/default/delayed-delivery-example-topic -n 5
   ```

6. Go to the terminal running the consumer example, you will see the following output. The consumer example successfully received
   10 messages. For not using `deliverAfter`, the difference between publish time and receive time is 0 seconds. For using `deliverAfter`, is 5 seconds.
    ```
    Consumer Received message : Immediate delivery message 0; Difference between publish time and receive time = 0 seconds
    Consumer Received message : Immediate delivery message 1; Difference between publish time and receive time = 0 seconds
    Consumer Received message : Immediate delivery message 2; Difference between publish time and receive time = 0 seconds
    Consumer Received message : Immediate delivery message 3; Difference between publish time and receive time = 0 seconds
    Consumer Received message : Immediate delivery message 4; Difference between publish time and receive time = 0 seconds
    Consumer Received message : DeliverAfter message 0; Difference between publish time and receive time = 5 seconds
    Consumer Received message : DeliverAfter message 1; Difference between publish time and receive time = 5 seconds
    Consumer Received message : DeliverAfter message 2; Difference between publish time and receive time = 5 seconds
    Consumer Received message : DeliverAfter message 3; Difference between publish time and receive time = 5 seconds
    Consumer Received message : DeliverAfter message 4; Difference between publish time and receive time = 5 seconds
    ```

7. Open another terminal, run the DelayedAtMessageProducer example to produce 10 messages to the topic .
   The producer example will produce the first 5 messages immediately and produce 5 messages delayed 10 seconds using `deliverAt`.
   ```
    SharpPulsar-Examples delayedatmessageproducer -t public/default/delayed-delivery-example-topic -n 5
   ```

8. Go to the terminal running the consumer example, you will see the following output. The consumer example successfully received
   10 messages. For not using `deliverAt`, the difference between publish time and receive time is 0 seconds. For using `deliverAt`, is 5 seconds.
    ```
    Consumer Received message : Immediate delivery message 0; Difference between publish time and receive time = 0 seconds
    Consumer Received message : Immediate delivery message 1; Difference between publish time and receive time = 0 seconds
    Consumer Received message : Immediate delivery message 2; Difference between publish time and receive time = 0 seconds
    Consumer Received message : Immediate delivery message 3; Difference between publish time and receive time = 0 seconds
    Consumer Received message : Immediate delivery message 4; Difference between publish time and receive time = 0 seconds
    Consumer Received message : DeliverAt message 0; Difference between publish time and receive time = 5 seconds
    Consumer Received message : DeliverAt message 1; Difference between publish time and receive time = 5 seconds
    Consumer Received message : DeliverAt message 2; Difference between publish time and receive time = 5 seconds
    Consumer Received message : DeliverAt message 3; Difference between publish time and receive time = 5 seconds
    Consumer Received message : DeliverAt message 4; Difference between publish time and receive time = 5 seconds
    ```
