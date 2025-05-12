
using Azure.Messaging.ServiceBus;
using BigFilesMessageQueues.Features.UploadImage;
using BigFilesMessageQueues.Features.UploadImage.Infrastructure.Persistence;
using BigFilesMessageQueues.Features.UploadImage.Services;

namespace BigFilesMessageQueues;

public class Program
{
    public static void Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        // Add services to the container.

        builder.Services.AddControllers();
        // Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
        builder.Services.AddOpenApi();

        builder.Services.AddSingleton<IFileStorageService, LocalFileStorageService>();
        builder.Services.AddSingleton<UploadImageHandler>();

        builder.Services.AddSingleton(serviceProvicer =>
        {
            var configuration = serviceProvicer.GetRequiredService<IConfiguration>();
            var connectionString = configuration["AzureServiceBus:ConnectionString"];
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("Azure Service Bus connection string is set");
            }
            return new ServiceBusClient(connectionString);
        });

        var app = builder.Build();

        // Configure the HTTP request pipeline.
        if (app.Environment.IsDevelopment())
        {
            app.MapOpenApi();
        }

        app.UseAuthorization();


        app.MapControllers();

        app.Run();
    }
}
