using SharpPulsar;
using SharpPulsar.Configuration;
using SharpPulsar.Exceptions;
using SharpPulsar.Interfaces;
using SharpPulsar.Interfaces.ISchema;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text.Json;
using System.Threading;

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

        private void Run(ConsumerFlags flags)
        {
            var clientConfig = new PulsarClientConfigBuilder()
            .ServiceUrl(flags.binaryServiceUrl);

            var pulsarSystem = PulsarSystem.GetInstance(clientConfig);

            var pulsarClient = pulsarSystem.NewClient();


            var builder = new ConsumerConfigBuilder<IGenericRecord>()
            .Topic(flags.topic)
                .SubscriptionName(flags.subscriptionName)
                .SubscriptionType(flags.subscriptionType)
                .SubscriptionInitialPosition(flags.subscriptionInitialPosition);

            var consumer = pulsarClient.NewConsumer(ISchema<object>.AutoConsume(), builder);

            Thread.Sleep(TimeSpan.FromSeconds(5));
            int numReceived = 0;
            try
            {
                while (flags.numMessages <= 0 || numReceived < flags.numMessages)
                {
                    var m = consumer.Receive();

                    if (m == null)
                    {
                        Thread.Sleep(TimeSpan.FromMilliseconds(100));
                        continue;
                    }
                    var receivedMessage = m.Value;
                    var feature = receivedMessage.GetField("Feature").ToString();
                    var strinData = (Dictionary<string, object>)receivedMessage.GetField("StringData");
                    var complexData = FromBytes<ComplexData>((byte[])receivedMessage.GetField("ComplexData"));
                    Console.WriteLine(JsonSerializer.Serialize(strinData, new JsonSerializerOptions { WriteIndented = true }));
                    Console.WriteLine(JsonSerializer.Serialize(complexData, new JsonSerializerOptions { WriteIndented = true }));
                    
                    consumer.Acknowledge(m);
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

        public static void Start(ConsumerFlags flags)
        {
            var example = new GenericConsumer();
            example.Run(flags);
        }
        // Convert a byte array to an Object
        private T FromBytes<T>(byte[] array)
        {
            var memStream = new MemoryStream();
            var binForm = new BinaryFormatter();
            memStream.Write(array, 0, array.Length);
            memStream.Seek(0, SeekOrigin.Begin);
            var obj = (T)binForm.Deserialize(memStream);

            return obj;
        }
    }
}
