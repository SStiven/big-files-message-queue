using Azure.Messaging.ServiceBus;

namespace BigFilesMessageQueues.DataProcessingService;

public class Program
{
    public static void Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);
        builder.Services.AddHostedService<ImageProcessingWorker>();

        builder.Services.AddSingleton(serviceProvicer =>
        {
            var configuration = serviceProvicer.GetRequiredService<IConfiguration>();
            var connectionString = configuration["AzureServiceBus:ConnectionString"];
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("Azure Service Bus connection string is not set");
            }
            return new ServiceBusClient(connectionString);
        });

        builder.Services.AddSingleton<IFileStorageService, LocalStorageService>();

        var host = builder.Build();
        host.Run();
    }
}