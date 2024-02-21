using SharpPulsar;
using SharpPulsar.Builder;
using SharpPulsar.Schemas;
using System;
using System.Text.Json;
using System.Threading.Tasks;

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

        private async Task Run(ProducerFlags flags)
        {
            var clientConfig = new PulsarClientConfigBuilder()
            .ServiceUrl(flags.binaryServiceUrl);

            var pulsarSystem = PulsarSystem.GetInstance();
            var pulsarClient = await pulsarSystem.NewClient(clientConfig);

            var schema = AvroSchema<SqlData>.Of(typeof(SqlData));
            var pBuilder = new ProducerConfigBuilder<SqlData>()
            .Topic(flags.topic);

            var producer = await pulsarClient.NewProducerAsync(schema, pBuilder);

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
               var receipt = await producer.SendAsync(data);
                Console.WriteLine(JsonSerializer.Serialize(receipt, new JsonSerializerOptions { WriteIndented = true}));
            }
        }

        public static async Task Start(ProducerFlags flags)
        {
            var example = new SqlProducer();
            await example.Run(flags);
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
