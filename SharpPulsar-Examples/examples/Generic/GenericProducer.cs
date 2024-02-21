using Avro.Generic;
using SharpPulsar;
using SharpPulsar.Builder;
using SharpPulsar.Interfaces.Schema;
using SharpPulsar.Schemas;
using SharpPulsar.Schemas.Generic;
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;

namespace SharpPulsar_Examples.examples.Generic
{
    public class GenericProducer : ExampleRunner<ProducerFlags>
    {
        protected internal override string Name()
        {
            return typeof(GenericProducer).Name;
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

            var schema = AvroSchema<ComplexGenericData>.Of(typeof(ComplexGenericData));
            var genericSchema = GenericAvroSchema.Of(schema.SchemaInfo);
            var pBuilder = new ProducerConfigBuilder<IGenericRecord>()
            .Topic(flags.topic);

            var producer = await pulsarClient.NewProducerAsync(genericSchema, pBuilder);

            int numMessages = Math.Max(flags.numMessages, 1);

            for (var i = 0; i < numMessages; i++)
            {
                var dataForWriter = new GenericRecord((Avro.RecordSchema)genericSchema.AvroSchema);
                dataForWriter.Add("Feature", "Education");
                dataForWriter.Add("StringData", new Dictionary<string, string> { { "Index", i.ToString() }, { "FirstName", "Ebere" }, { "LastName", "Abanonu" } });
                dataForWriter.Add("ComplexData", ToBytes(new ComplexData { ProductId = i, Point = i * 2, Sales = i * 2 * 5 }));
                var record = new GenericAvroRecord(null, genericSchema.AvroSchema, genericSchema.Fields, dataForWriter);
                var receipt = await producer.SendAsync(record);
                Console.WriteLine(JsonSerializer.Serialize(receipt, new JsonSerializerOptions { WriteIndented = true}));
            }
        }

        public static async Task Start(ProducerFlags flags)
        {
            var example = new GenericProducer();
            await example.Run(flags);
        }
        private byte[] ToBytes<T>(T obj)
        {
            if (obj == null)
                return null;

            return JsonSerializer.SerializeToUtf8Bytes(obj,
                     new JsonSerializerOptions { WriteIndented = false, IgnoreNullValues = true });
        }

    }
    public class ComplexGenericData
    {
        public string Feature { get; set; }
        public Dictionary<string, string> StringData { get; set; }
        public byte[] ComplexData { get; set; }

    }
    [Serializable]
    public class ComplexData
    {
        public int ProductId { get; set; }
        public int Point { get; set; }
        public long Sales { get; set; }
    }
}
