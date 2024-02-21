using Akka.Actor;
using SharpPulsar.Trino;
using SharpPulsar.Trino.Message;
using System;
using System.Text.Json;
using System.Threading.Tasks;

namespace SharpPulsar_Examples.examples.Sql
{
    public class SqlQuery : ExampleRunner<SqlFlags>
    {

        protected internal override string Name()
        {
            return typeof(SqlQuery).Name;
        }

        protected internal override string Description()
        {
            return "An example demonstrates how to use generic record feature";
        }

        protected internal override SqlFlags Flags()
        {
            return new SqlFlags();
        }

        private async Task Run(SqlFlags flags)
        {
            var option = new ClientOptions { Server = flags.ServerAddress, Execute = flags.Query, Catalog = "pulsar", Schema = $"{flags.Tenant}/{flags.Namespace}" };
           
            var actorSystem = ActorSystem.Create("Sql");
            //var sql = PulsarSystem.Sql(option);
            var sql = new SqlInstance(actorSystem, option);
            var response = await sql.ExecuteAsync(TimeSpan.FromSeconds(30));
            if (response != null)
            {
                var data = response.Response;
                switch (data)
                {
                    case DataResponse dr:
                        {
                            for (var i = 0; i < dr.Data.Count; i++)
                            {
                                Console.WriteLine(JsonSerializer.Serialize(dr.Data[i], new JsonSerializerOptions { WriteIndented = true }));
                            }
                            Console.WriteLine(JsonSerializer.Serialize(dr.StatementStats, new JsonSerializerOptions { WriteIndented = true }));
                        }
                        break;
                    case StatsResponse sr:
                        Console.WriteLine(JsonSerializer.Serialize(sr.Stats, new JsonSerializerOptions { WriteIndented = true }));
                        break;
                    case ErrorResponse er:
                        Console.WriteLine(JsonSerializer.Serialize(er, new JsonSerializerOptions { WriteIndented = true }));
                        break;
                }
            }
        }

        public static async Task Start(SqlFlags flags)
        {
            var example = new SqlQuery();
            await example.Run(flags);
        }
       
    }
}
