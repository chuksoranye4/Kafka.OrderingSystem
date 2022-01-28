using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Domain.AppSettings;
using Domain.Kafka;
using Domain.Models;
using Microsoft.Extensions.Configuration;

namespace Service.Ordering
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var configuartion = new ConfigurationBuilder().AddJsonFile("appsettings.json", optional: false, reloadOnChange: true).Build();

            var producerSubmittedTopicName = AppSettings.GetTopicName(configuartion, "Submitted");
            var producerReportedTopicName = AppSettings.GetTopicName(configuartion, "Reported");

            var _kafkaService = new KafkaService(configuartion);

            while (true)
            {
                var (order, report) = DoOrdering();
                string jsonData = JsonSerializer.Serialize(report);

                var (result, error) = await _kafkaService.Publish(producerReportedTopicName, jsonData);

                jsonData = JsonSerializer.Serialize(order);

                (result, error) = await _kafkaService.Publish(producerSubmittedTopicName, jsonData);

            }
        }

        private static (Order, Report) DoOrdering()
        {
            var random = new Random();
            Thread.Sleep(1000);
            var pid = random.Next(111111, 999999);
            var order = new Order()
            {
                Id = Guid.NewGuid(),
                ProductId = pid,
                ProductName = $"Product {pid}",
                Quantity = random.Next(1, 10),
                Price = random.Next(1, 100),
                CreatedOn = DateTime.Now
            };

            var report = new Report()
            {
                Id = Guid.NewGuid(),
                Order = order,
                Details = $"Order has been submitted: ${order.ProductName}",
                CreatedOn = DateTime.Now
            };
            return (order, report);
        }

    }
}
