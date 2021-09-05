using SharpPulsar;
using SharpPulsar.Configuration;
using SharpPulsar.Exceptions;
using SharpPulsar.Interfaces;
using System;
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
/// <summary>
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
/// <para>
/// http://www.apache.org/licenses/LICENSE-2.0
/// </para>
/// <para>
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
/// </para>
/// </summary>
namespace SharpPulsar_Examples.examples.DelayedMessages
{
    /// <summary>
    /// Example that demonstrates how to use delayed message delivery feature.
    /// 
    /// </summary>
    public class DelayedMessageConsumer : ExampleRunner<ConsumerFlags>
    {
        protected internal override string Name()
        {
            return typeof(DelayedMessageConsumer).Name;
        }

        protected internal override string Description()
        {
            return "An example demonstrates how to use delayed message delivery feature";
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
            var consumer = pulsarClient.NewConsumer(ISchema<string>.String, new ConsumerConfigBuilder<string>()
                .Topic(flags.topic)
                .SubscriptionName(flags.subscriptionName)
                .SubscriptionType(flags.subscriptionType)
                .SubscriptionInitialPosition(flags.subscriptionInitialPosition));
            int numReceived = 0;
            try
            {
                while (flags.numMessages <= 0 || numReceived < flags.numMessages)
                {
                    var msg = consumer.Receive();
                    if (msg == null)
                        continue;
                    Console.WriteLine("Consumer Received message : " + msg.Data + "; Difference between publish time and receive time = " + (DateTimeHelper.CurrentUnixTimeMillis() - msg.PublishTime) / 1000 + " seconds");
                    consumer.Acknowledge(msg);
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
            DelayedMessageConsumer example = new DelayedMessageConsumer();
            example.Run(flags);
        }
    }
}
