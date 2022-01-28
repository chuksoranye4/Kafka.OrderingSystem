using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Configuration;

namespace Domain.AppSettings
{
    public static class AppSettings
    {
        private const string keyPrefix = "KafkaSetting";

        public static List<KeyValuePair<string, string>> GetConfig(IConfiguration configuration, string key)
        {

            return configuration.GetSection($"{keyPrefix}:{key}").GetChildren()
                .ToDictionary(p => p.Key, p => p.Value).ToList();
        }

        public static string GetTopicName(IConfiguration configuration,string key)
        {
            return configuration.GetSection($"{keyPrefix}:Producer:{key}").GetChildren()
                .ToDictionary(p => p.Key, p => p.Value).ToList()
                ?.Where(x => x.Key.Equals("TopicName")).FirstOrDefault().Value;
        }

    }
}
