﻿using SharpPulsar_Examples.examples.DelayedMessages;
using SharpPulsar_Examples.examples.Generic;
using SharpPulsar_Examples.examples.Sql;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace SharpPulsar_Examples
{
    class Program
    {
        //SharpPulsar-Examples delayedaftermessageproducer -t public/default/delayed-delivery-topic -n 10
        //SharpPulsar-Examples delayedatmessageproducer -t public/default/delayed-delivery-topic -n 10
        //SharpPulsar-Examples DelayedMessageConsumer -t public/default/delayed-delivery-topic -sn test-sub -st Shared -n 0

        //Generic Record
        //SharpPulsar-Examples GenericProducer -t public/default/generictopict -n 10
        //SharpPulsar-Examples GenericConsumer -t public/default/generictopict -sn generic-sub -st Shared -n 0

        //Sql
        //SharpPulsar-Examples SqlProducer -t public/default/sqltopic -n 100
        //SharpPulsar-Examples SqlQuery -sr "http://127.0.0.1:8081" -q "select * from sqltopic" -tn public -ns default
        //SharpPulsar-Examples SqlLiveQuery -sr "http://127.0.0.1:8081" -q "select * from sqltopic where __publish_time__ > {time} LIMIT 50" -tn public -ns default -t sqltopic
        static async Task Main(string[] args)
        {
            var token = new CancellationTokenSource();
            if (args.Length == 0)
            {
                Console.WriteLine("Invalid args length");
                return;
            }
            var command = args[0];

            switch (command.ToLower())
            {
                case "delayedaftermessageproducer" when args[1] == "-t" && !string.IsNullOrWhiteSpace(args[2]) && args[3] == "-n" && !string.IsNullOrWhiteSpace(args[4]):
                    await DelayedAfterMessageProducer.Start(new ProducerFlags
                    {
                        topic = args[2],
                        numMessages = int.Parse(args[4])
                    }); 
                    break;
                case "genericproducer" when args[1] == "-t" && !string.IsNullOrWhiteSpace(args[2]) && args[3] == "-n" && !string.IsNullOrWhiteSpace(args[4]):
                    await GenericProducer.Start(new ProducerFlags
                    {
                        topic = args[2],
                        numMessages = int.Parse(args[4])
                    }); 
                    break;
                case "sqlproducer" when args[1] == "-t" && !string.IsNullOrWhiteSpace(args[2]) && args[3] == "-n" && !string.IsNullOrWhiteSpace(args[4]):
                    await SqlProducer.Start(new ProducerFlags
                    {
                        topic = args[2],
                        numMessages = int.Parse(args[4])
                    }); 
                    break;
                case "sqlquery" when args[1] == "-sr" && !string.IsNullOrWhiteSpace(args[2]) && args[3] == "-q" && !string.IsNullOrWhiteSpace(args[4]) && args[5] == "-tn" && !string.IsNullOrWhiteSpace(args[6]) && args[7] == "-ns" && !string.IsNullOrWhiteSpace(args[8]):
                    await SqlQuery.Start(new SqlFlags
                    {
                        ServerAddress = args[2],
                        Query = args[4],
                        Tenant = args[6],
                        Namespace = args[8]
                    }) ;
                    break;
                case "sqllivequery" when args[1] == "-sr" && !string.IsNullOrWhiteSpace(args[2]) && args[3] == "-q" && !string.IsNullOrWhiteSpace(args[4]) && args[5] == "-tn" && !string.IsNullOrWhiteSpace(args[6]) && args[7] == "-ns" && !string.IsNullOrWhiteSpace(args[8]) && args[9] == "-t" && !string.IsNullOrWhiteSpace(args[10]):
                    await SqlLiveQuery.Start(new SqlFlags
                    {
                        topic = args[10],
                        ServerAddress = args[2],
                        Query = args[4],
                        Tenant = args[6],
                        Namespace = args[8]
                    }, token.Token) ;
                    break;
                case "delayedatmessageproducer" when args[1] == "-t" && !string.IsNullOrWhiteSpace(args[2]) && args[3] == "-n" && !string.IsNullOrWhiteSpace(args[4]):
                    await DelayedAtMessageProducer.Start(new ProducerFlags
                    {
                        topic = args[2],
                        numMessages = int.Parse(args[4])
                    }); 
                    break;
                case "delayedmessageconsumer" when args[1] == "-t" && !string.IsNullOrWhiteSpace(args[2]) && args[3] == "-sn" && !string.IsNullOrWhiteSpace(args[4]) && args[5] == "-st" && !string.IsNullOrWhiteSpace(args[6]) && args[7] == "-n" && !string.IsNullOrWhiteSpace(args[8]):
                    await DelayedMessageConsumer.Start(new ConsumerFlags
                    {
                        topic = args[2],
                        subscriptionName = args[4],
                        subscriptionType = Sub(args[6]),
                        numMessages = int.Parse(args[8])
                        
                    });
                    break;
                case "genericconsumer" when args[1] == "-t" && !string.IsNullOrWhiteSpace(args[2]) && args[3] == "-sn" && !string.IsNullOrWhiteSpace(args[4]) && args[5] == "-st" && !string.IsNullOrWhiteSpace(args[6]) && args[7] == "-n" && !string.IsNullOrWhiteSpace(args[8]):
                    await GenericConsumer.Start(new ConsumerFlags
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
            token.Cancel();
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
