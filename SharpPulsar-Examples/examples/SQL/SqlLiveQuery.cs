using Akka.Actor;
using SharpPulsar.Sql.Client;
using SharpPulsar.Sql.Message;
using SharpPulsar.Sql.Public;
using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace SharpPulsar_Examples.examples.Sql
{
    public class SqlLiveQuery : ExampleRunner<SqlFlags>
    {
        private readonly CancellationToken _token;
        public SqlLiveQuery(CancellationToken token)
        {
            _token = token;
        }
        protected internal override string Name()
        {
            return typeof(SqlLiveQuery).Name;
        }

        protected internal override string Description()
        {
            return "An example demonstrates how to use generic record feature";
        }

        protected internal override SqlFlags Flags()
        {
            return new SqlFlags();
        }

        private async ValueTask Run(SqlFlags flags)
        {
            var option = new ClientOptions { Server = flags.ServerAddress, Execute = flags.Query, Catalog = "pulsar", Schema = $"{flags.Tenant}/{flags.Namespace}" };
            var actorSystem = ActorSystem.Create("Sql");
            //var sql = PulsarSystem.Sql(option);
            var sql = new LiveSqlInstance(actorSystem, option, flags.topic, queryInterval: TimeSpan.FromMilliseconds(5000), startAtPublishTime: DateTime.Now.AddMinutes(-30));

           while(!_token.IsCancellationRequested)
            {
                await foreach (var data in sql.ExecuteAsync(_token))
                {
                    Console.WriteLine(JsonSerializer.Serialize((DataResponse)data.Response, new JsonSerializerOptions { WriteIndented = true }));
                }
            }
        }

        public static void Start(SqlFlags flags, CancellationToken token)
        {
            var example = new SqlLiveQuery(token);
            example.Run(flags).GetAwaiter().GetResult();
        }
       
    }
}
