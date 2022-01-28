using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Domain.AppSettings;
using Domain.Kafka;
using Domain.Models;
using Microsoft.Extensions.Configuration;

namespace Service.Dispatch
{
    class Program
    {
        static async Task Main(string[] args)
        {
            ConsumeResult<Null, string> subResult;
            DeliveryResult<Null, string> pubResult;
            string error;

            var configuartion = new ConfigurationBuilder().AddJsonFile("appsettings.json", optional: false, reloadOnChange: true).Build();
        
            var producerReportedTopicName = AppSettings.GetTopicName(configuartion, "Reported");

            var _kafkaService = new KafkaService(configuartion);

            while (true)
            {
                (subResult, error) = _kafkaService.Subscribe();

                if (string.IsNullOrWhiteSpace(error))
                {
                    var order = JsonSerializer.Deserialize<Order>(subResult.Message.Value);

                    var report = DoDispatch(order);

                    string jsonData = JsonSerializer.Serialize(report);

                    (pubResult, error) = await _kafkaService.Publish(producerReportedTopicName, jsonData);
                }
            }
        }


        private static Report DoDispatch(Order order)
        {
            Thread.Sleep(1);
            return new Report()
            {
                Id = Guid.NewGuid(),
                Order = order,
                Details = $"Order has been Dispatched: ${order.ProductName}",
                Status = Status.OrderDispatched,
                CreatedOn = DateTime.Now
            };
        }
    }
}
