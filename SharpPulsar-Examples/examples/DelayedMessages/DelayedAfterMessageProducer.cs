using SharpPulsar;
using SharpPulsar.Configuration;
using SharpPulsar.Interfaces;
using System;

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
    public class DelayedAfterMessageProducer : ExampleRunner<ProducerFlags>
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

        private void Run(ProducerFlags flags)
        {
            var clientConfig = new PulsarClientConfigBuilder()
            .ServiceUrl(flags.binaryServiceUrl);

            var pulsarSystem = PulsarSystem.GetInstance(clientConfig);

            var pulsarClient = pulsarSystem.NewClient();
            var producer = pulsarClient.NewProducer(ISchema<string>.String, new ProducerConfigBuilder<string>()
                .Topic(flags.topic));
            int numMessages = Math.Max(flags.numMessages, 1);

            // immediate delivery
            for (int i = 0; i < numMessages; i++)
            {
                producer.NewMessage().Value("Immediate delivery message " + i).Send();
            }
            producer.Flush();

            // delay 5 seconds using DeliverAfter
            for (int i = 0; i < numMessages; i++)
            {
                producer.NewMessage().Value("DeliverAfter message " + i).DeliverAfter(TimeSpan.FromMilliseconds(5000)).Send();
            }
            producer.Flush();
        }

        public static void Start(ProducerFlags flags)
        {
            DelayedAfterMessageProducer example = new DelayedAfterMessageProducer();
            example.Run(flags);
        }
    }
}
