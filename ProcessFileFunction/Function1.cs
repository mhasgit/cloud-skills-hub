using System.Text;
using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Text.Json.Serialization;
using System.Xml;
using Newtonsoft.Json;

namespace ProcessFileFunction
{
    public class Function1
    {
        private readonly ILogger<Function1> _logger;

        public Function1(ILogger<Function1> logger)
        {
            _logger = logger;
        }

        [Function(nameof(Function1))]
        public async Task Run([BlobTrigger("input-files/{name}", Connection = "AzureWebJobsStorage")] Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($"Processing file {name}");
            
            using(var reader = new StreamReader(myBlob))
            {
                string csvData = reader.ReadToEnd();
                var jsonData = ConvertCsvToJson(csvData);
                var blobServiceClient = new BlobServiceClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
                var blobClient = blobServiceClient.GetBlobContainerClient("processed-files").GetBlobClient($"{name}.json");

                using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(jsonData)))
                {
                    blobClient.UploadAsync(stream, overwrite: true);
                }

                log.LogInformation($"Processed file saved as JSON: {name}.json");
            }
        }

        private static string ConvertCsvToJson(string csv)
        {
            var lines = csv.Split("\n");
            var headers = lines[0].Split(",");
            var data = new List<Dictionary<string, string>>();

            for (int i = 1; i < lines.Length; i++)
            {
                if (string.IsNullOrWhiteSpace(lines[i])) continue;

                var values = lines[i].Split(",");
                var obj = new Dictionary<string, string>();
                for (int j = 0; j < headers.Length; j++)
                {
                    if (j < values.Length)
                    {
                        obj[headers[j].Trim()] = values[j].Trim();
                    }
                }
                data.Add(obj);
            }
            return JsonConvert.SerializeObject(data, Newtonsoft.Json.Formatting.Indented);
        }

    }
}
