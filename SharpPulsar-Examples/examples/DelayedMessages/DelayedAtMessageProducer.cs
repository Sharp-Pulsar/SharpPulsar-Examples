using SharpPulsar;
using SharpPulsar.Builder;
using SharpPulsar.Interfaces;
using System;
using System.Threading.Tasks;

/// <summary>
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
/// 
///     http://www.apache.org/licenses/LICENSE-2.0
/// 
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
/// </summary>
namespace SharpPulsar_Examples.examples.DelayedMessages
{

    /// <summary>
    /// Example that demonstrates how to use delayed message delivery feature.
    /// 
    /// </summary>
    public class DelayedAtMessageProducer : ExampleRunner<ProducerFlags>
    {
        protected internal override string Name()
        {
            return typeof(DelayedAfterMessageProducer).Name;
        }

        protected internal override string Description()
        {
            return "An example demonstrates how to use delayed message delivery feature";
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
            var producer = await pulsarClient.NewProducerAsync(ISchema<string>.String, new ProducerConfigBuilder<string>()
                .Topic(flags.topic));

            int numMessages = Math.Max(flags.numMessages, 1);

            // immediate delivery
            for (int i = 0; i < numMessages; i++)
            {
                await producer.NewMessage().Value("Immediate delivery message " + i).SendAsync();
            }
            producer.Flush();

            // delay 5 seconds using DeliverAt
            for (int i = 0; i < numMessages; i++)
            {
                await producer.NewMessage().Value("DeliverAt message " + i).DeliverAt(DateTimeOffset.Now.AddSeconds(5)).SendAsync();
            }
            producer.Flush();
        }

        public static async Task Start(ProducerFlags flags)
        {
            DelayedAtMessageProducer example = new DelayedAtMessageProducer();
            await example.Run(flags);
        }
    }

}

