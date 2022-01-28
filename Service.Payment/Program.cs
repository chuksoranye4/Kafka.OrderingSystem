using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Domain.AppSettings;
using Domain.Kafka;
using Domain.Models;
using Microsoft.Extensions.Configuration;

namespace Service.Payment
{
    class Program
    {
        static async Task Main(string[] args)
        {
            ConsumeResult<Null, string> subResult;
            DeliveryResult<Null, string> pubResult;
            string error;

            var configuartion = new ConfigurationBuilder().AddJsonFile("appsettings.json", optional: false, reloadOnChange: true).Build();
            var producerProcessedTopicName = AppSettings.GetTopicName(configuartion, "Processed");
            var producerReportedTopicName = AppSettings.GetTopicName(configuartion, "Reported");

            var _kafkaService = new KafkaService(configuartion);

            while (true)
            {
                (subResult, error) = _kafkaService.Subscribe();

                if (string.IsNullOrWhiteSpace(error))
                {
                    var order = JsonSerializer.Deserialize<Order>(subResult.Message.Value);

                    var (report, isProcessed) = DoPaymentProcess(order);

                    string jsonData = JsonSerializer.Serialize(report);

                    (pubResult, error) = await _kafkaService.Publish(producerReportedTopicName, jsonData);

                    if (isProcessed)
                    {
                        (pubResult, error) = await _kafkaService.Publish(producerProcessedTopicName, subResult.Message.Value);
                    }
                }
            }

        }

        private static (Report, bool) DoPaymentProcess(Order order)
        {
            var isProcessed = false;

            Thread.Sleep(10000);

            var report = new Report()
            {
                Id = Guid.NewGuid(),
                Order = order,
                CreatedOn = DateTime.Now
            };


            if (order.Price > 50)
            {
                report.Details = "Order has Not been Processed due to failed payment.";
                report.Status = Status.PaymentFailed;
            }
            else
            {
                report.Details = "Product has been processed";
                report.Status = Status.PaymentProcessed;

                isProcessed = true;
            }


            return (report, isProcessed);
        }
    }
}
