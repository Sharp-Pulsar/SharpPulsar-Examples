using SharpPulsar;
using SharpPulsar.Builder;
using SharpPulsar.Exceptions;
using SharpPulsar.Interfaces;
using SharpPulsar.Interfaces.Schema;
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace SharpPulsar_Examples.examples.Generic
{
    public class GenericConsumer : ExampleRunner<ConsumerFlags>
    {

        protected internal override string Name()
        {
            return typeof(GenericConsumer).Name;
        }

        protected internal override string Description()
        {
            return "An example demonstrates how to use generic record feature";
        }

        protected internal override ConsumerFlags Flags()
        {
            return new ConsumerFlags();
        }

        private async Task Run(ConsumerFlags flags)
        {
            var clientConfig = new PulsarClientConfigBuilder()
            .ServiceUrl(flags.binaryServiceUrl);

            var pulsarSystem = PulsarSystem.GetInstance();

            var pulsarClient = await pulsarSystem.NewClient(clientConfig);


            var builder = new ConsumerConfigBuilder<IGenericRecord>()
            .Topic(flags.topic)
                .SubscriptionName(flags.subscriptionName)
                .SubscriptionType(flags.subscriptionType)
                .SubscriptionInitialPosition(flags.subscriptionInitialPosition);

            var consumer = await pulsarClient.NewConsumerAsync(ISchema<object>.AutoConsume(), builder);

            await Task.Delay(TimeSpan.FromSeconds(5));
            int numReceived = 0;
            try
            {
                while (flags.numMessages <= 0 || numReceived < flags.numMessages)
                {
                    var m = await consumer.ReceiveAsync();

                    if (m == null)
                    {
                        await Task.Delay(TimeSpan.FromMilliseconds(100));
                        continue;
                    }
                    var receivedMessage = m.Value;
                    var feature = receivedMessage.GetField("Feature").ToString();
                    var strinData = (Dictionary<string, object>)receivedMessage.GetField("StringData");
                    var complexData = FromBytes<ComplexData>((byte[])receivedMessage.GetField("ComplexData"));
                    Console.WriteLine(JsonSerializer.Serialize(strinData, new JsonSerializerOptions { WriteIndented = true }));
                    Console.WriteLine(JsonSerializer.Serialize(complexData, new JsonSerializerOptions { WriteIndented = true }));
                    
                    await consumer.AcknowledgeAsync(m);
                    ++numReceived;
                }

                Console.WriteLine("Successfully received " + numReceived + " messages");
            }
            catch (PulsarClientException ie)
            {
                Console.WriteLine("Successfully received " + numReceived + " messages");
                Console.WriteLine(ie.ToString());
            }
        }

        public static async Task Start(ConsumerFlags flags)
        {
            var example = new GenericConsumer();
            await example.Run(flags);
        }
        // Convert a byte array to an Object
        private T FromBytes<T>(byte[] array)
        {
            return JsonSerializer.Deserialize<T>(new ReadOnlySpan<byte>(array));
        }
    }
}
