using SharpPulsar;
using SharpPulsar.Configuration;
using SharpPulsar.Schemas;
using System;
using System.Text.Json;

namespace SharpPulsar_Examples.examples.Sql
{
    public class SqlProducer : ExampleRunner<ProducerFlags>
    {
        protected internal override string Name()
        {
            return typeof(SqlProducer).Name;
        }

        protected internal override string Description()
        {
            return "An example demonstrates how to use generic record feature";
        }

        protected internal override ProducerFlags Flags()
        {
            return new ProducerFlags();
        }

        private void Run(ProducerFlags flags)
        {
            var clientConfig = new PulsarClientConfigBuilder()
            .ServiceUrl(flags.binaryServiceUrl);

            var pulsarSystem = PulsarSystem.GetInstance(clientConfig);
            var pulsarClient = pulsarSystem.NewClient();

            var schema = AvroSchema<SqlData>.Of(typeof(SqlData));
            var pBuilder = new ProducerConfigBuilder<SqlData>()
            .Topic(flags.topic);

            var producer = pulsarClient.NewProducer(schema, pBuilder);

            int numMessages = Math.Max(flags.numMessages, 1);

            for (var i = 0; i < numMessages; i++)
            {
                var data = new SqlData
                {
                    ProductId = i,
                    Country = "Nigeria",
                    Point = 6*i,
                    Sales = 10*2*i
                };
               var receipt = producer.Send(data);
                Console.WriteLine(JsonSerializer.Serialize(receipt, new JsonSerializerOptions { WriteIndented = true}));
            }
        }

        public static void Start(ProducerFlags flags)
        {
            var example = new SqlProducer();
            example.Run(flags);
        }
       
    }
    public class SqlData
    {
        public int ProductId { get; set; }
        public int Point { get; set; }
        public long Sales { get; set; }
        public string Country { get; set; }
    }
}
