using System;
using System.Text.Json;
using Confluent.Kafka;
using Domain.Kafka;
using Domain.Models;
using Microsoft.Extensions.Configuration;

namespace Service.Reporting
{
    class Program
    {
        static void Main(string[] args)
        {
            ConsumeResult<Null, string> subResult;
            string error;

            var configuartion = new ConfigurationBuilder().AddJsonFile("appsettings.json", optional: false, reloadOnChange: true).Build();
            
            var _kafkaService = new KafkaService(configuartion);

            while (true)
            {
                (subResult, error) = _kafkaService.Subscribe();

                if (string.IsNullOrWhiteSpace(error))
                {
                    var report = JsonSerializer.Deserialize<Report>(subResult.Message.Value);
                    Console.WriteLine($"[Report Status: {report.Status}] => CreatedOn: {report.CreatedOn}, Report Id: {report.Id}, Report Details: {report.Details}");
                }
            }
        }

    }
}
