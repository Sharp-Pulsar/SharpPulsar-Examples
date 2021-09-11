using SharpPulsar_Examples.examples.DelayedMessages;
using SharpPulsar_Examples.examples.Generic;
using System;

namespace SharpPulsar_Examples
{
    class Program
    {
        //SharpPulsar-Examples delayedaftermessageproducer -t public/default/delayed-delivery-example-topic -n 10
        //SharpPulsar-Examples delayedatmessageproducer -t public/default/delayed-delivery-example-topic -n 10
        //SharpPulsar-Examples DelayedMessageConsumer -t public/default/delayed-delivery-example-topic -sn test-sub -st Shared -n 20

        //Generic Record
        //SharpPulsar-Examples GenericProducer -t public/default/generic-topic-2 -n 10
        //SharpPulsar-Examples GenericConsumer -t public/default/generic-topic-2 -sn generic-sub -st Shared -n 0
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("Invalid args length");
                return;
            }
            var command = args[0];

            switch (command.ToLower())
            {
                case "delayedaftermessageproducer" when args[1] == "-t" && !string.IsNullOrWhiteSpace(args[2]) && args[3] == "-n" && !string.IsNullOrWhiteSpace(args[4]):
                    DelayedAfterMessageProducer.Start(new ProducerFlags
                    {
                        topic = args[2],
                        numMessages = int.Parse(args[4])
                    }); 
                    break;
                case "genericproducer" when args[1] == "-t" && !string.IsNullOrWhiteSpace(args[2]) && args[3] == "-n" && !string.IsNullOrWhiteSpace(args[4]):
                    GenericProducer.Start(new ProducerFlags
                    {
                        topic = args[2],
                        numMessages = int.Parse(args[4])
                    }); 
                    break;
                case "delayedatmessageproducer" when args[1] == "-t" && !string.IsNullOrWhiteSpace(args[2]) && args[3] == "-n" && !string.IsNullOrWhiteSpace(args[4]):
                    DelayedAtMessageProducer.Start(new ProducerFlags
                    {
                        topic = args[2],
                        numMessages = int.Parse(args[4])
                    }); 
                    break;
                case "delayedmessageconsumer" when args[1] == "-t" && !string.IsNullOrWhiteSpace(args[2]) && args[3] == "-sn" && !string.IsNullOrWhiteSpace(args[4]) && args[5] == "-st" && !string.IsNullOrWhiteSpace(args[6]) && args[7] == "-n" && !string.IsNullOrWhiteSpace(args[8]):
                    DelayedMessageConsumer.Start(new ConsumerFlags
                    {
                        topic = args[2],
                        subscriptionName = args[4],
                        subscriptionType = Sub(args[6]),
                        numMessages = int.Parse(args[8])
                        
                    });
                    break;
                case "genericconsumer" when args[1] == "-t" && !string.IsNullOrWhiteSpace(args[2]) && args[3] == "-sn" && !string.IsNullOrWhiteSpace(args[4]) && args[5] == "-st" && !string.IsNullOrWhiteSpace(args[6]) && args[7] == "-n" && !string.IsNullOrWhiteSpace(args[8]):
                    GenericConsumer.Start(new ConsumerFlags
                    {
                        topic = args[2],
                        subscriptionName = args[4],
                        subscriptionType = Sub(args[6]),
                        numMessages = int.Parse(args[8])
                        
                    }); 
                    break;
                default:
                    Console.WriteLine("Invalid command");
                    break;
            }
        }
        private static SharpPulsar.Protocol.Proto.CommandSubscribe.SubType Sub(string sub)
        {
            switch(sub.ToLower())
            {
                case "shared":
                    return SharpPulsar.Protocol.Proto.CommandSubscribe.SubType.Shared;
                case "keyshared":
                    return SharpPulsar.Protocol.Proto.CommandSubscribe.SubType.KeyShared;
                case "failover":
                    return SharpPulsar.Protocol.Proto.CommandSubscribe.SubType.Failover;
                default:
                    return SharpPulsar.Protocol.Proto.CommandSubscribe.SubType.Exclusive;
            }
        }
    }
}
