using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Domain.AppSettings;
using Domain.Kafka;
using Domain.Models;
using Microsoft.Extensions.Configuration;

namespace Service.Inventory
{
    class Program
    {
        static async Task Main(string[] args)
        {
            ConsumeResult<Null, string> subResult;
            DeliveryResult<Null, string> pubResult;
            string error;

            var configuartion = new ConfigurationBuilder().AddJsonFile("appsettings.json", optional: false, reloadOnChange: true).Build();
            var producerValidatedTopicName = AppSettings.GetTopicName(configuartion, "Validated");
            var producerReportedTopicName = AppSettings.GetTopicName(configuartion, "Reported");

            var _kafkaService = new KafkaService(configuartion);

            while (true)
            {
                (subResult, error) = _kafkaService.Subscribe();

                if (string.IsNullOrWhiteSpace(error))
                {
                    var order = JsonSerializer.Deserialize<Order>(subResult.Message.Value);

                    var (report, isvalidate) = DoInventory(order);

                    string jsonData = JsonSerializer.Serialize(report);

                    (pubResult, error) = await _kafkaService.Publish(producerReportedTopicName, jsonData);

                    if (isvalidate)
                    {
                        (pubResult, error) = await _kafkaService.Publish(producerValidatedTopicName, subResult.Message.Value);
                    }
                }
            }

        }

        private static (Report, bool) DoInventory(Order order)
        {
            bool isValidated = false;
            Thread.Sleep(1);

            var report = new Report()
            {
                Id = Guid.NewGuid(),
                Order = order,
                Details = "Order has NOT been validated due to out of stock.",
                Status = Status.OrderOutOfStock,
                CreatedOn = DateTime.Now
            };

            if (order.Quantity > 6)
            {
                report.Details = "Order has been validated";
                report.Status = Status.OrderValidated;
                isValidated = true;
            }

            return (report, isValidated);
        }

    }
}
