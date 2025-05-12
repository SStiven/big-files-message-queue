using System.Text.Json;
using Azure.Messaging.ServiceBus;
using BigFilesMessageQueues.Features.UploadImage.Services;

namespace BigFilesMessageQueues.Features.UploadImage.Infrastructure.Persistence;

public class LocalFileStorageService : IFileStorageService
{
    private readonly string _baseStoragePath;
    private readonly ILogger<LocalFileStorageService> _logger;
    private readonly ServiceBusSender _serviceBusSender;

    public LocalFileStorageService(
        IConfiguration configuration,
        ILogger<LocalFileStorageService> logger,
        ServiceBusClient serviceBusClient)
    {

        _logger = logger;

        ArgumentNullException.ThrowIfNull(configuration);

        _baseStoragePath = configuration["FileUploads:StagingAreaPath"];

        if (string.IsNullOrWhiteSpace(_baseStoragePath))
        {
            _logger.LogError("FileUploads:StagingAreaPath configuration is missing or empty.");
            throw new InvalidOperationException("Staging area path is not configured.");
        }

        if (!Directory.Exists(_baseStoragePath))
        {
            try
            {
                Directory.CreateDirectory(_baseStoragePath);
                _logger.LogInformation("Created staging directory at: {Path}", _baseStoragePath);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to create staging directory at: {Path}", _baseStoragePath);
                throw;
            }
        }

        var queueName = configuration["AzureServiceBus:UploadNotificationQueueName"];
        if (string.IsNullOrEmpty(queueName))
        {
            throw new InvalidOperationException("UploadNotificationQueueName is not configured.");
        }

        _serviceBusSender = serviceBusClient.CreateSender(queueName);
    }

    public async Task<string> SaveFileAsync(Stream fileStream, string originalFileName, string contentType, CancellationToken cancellationToken = default)
    {
        if (fileStream == null || fileStream.Length == 0)
        {
            throw new ArgumentException("File stream cannot be null or empty.", nameof(fileStream));
        }
        if (string.IsNullOrWhiteSpace(originalFileName))
        {
            throw new ArgumentException("Original file name must be provided", nameof(originalFileName));
        }

        try
        {
            var extension = Path.GetExtension(originalFileName);
            if (string.IsNullOrEmpty(extension))
            {
                _logger.LogWarning("Original filename '{OriginalFileName}' has no extension, saving without one", originalFileName);
            }

            var uniqueFileName = $"{Guid.NewGuid()}{extension}";
            var fullPath = Path.Combine(_baseStoragePath, uniqueFileName);

            _logger.LogInformation("Attempting to save file '{OriginalFileName}' as '{UniqueFileName}' to '{FullPath}'", originalFileName, uniqueFileName, fullPath);

            fileStream.Seek(0, SeekOrigin.Begin);
            using (var outputFileStream = new FileStream(fullPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 4096, useAsync: true))
            {
                await fileStream.CopyToAsync(outputFileStream);
            }

            _logger.LogInformation("Successfully saved file as '{UniqueFileName}' to '{FullPath}'", uniqueFileName, fullPath);

            var notification = new FileUploadNotification(
                Guid.NewGuid().ToString(),
                uniqueFileName,
                originalFileName,
                contentType,
                DateTime.UtcNow
            );

            var messageBody = JsonSerializer.Serialize(notification);
            var serviceBusMessage = new ServiceBusMessage(messageBody)
            {
                ContentType = "application/json",
                MessageId = notification.FileId,
            };

            await _serviceBusSender.SendMessageAsync(serviceBusMessage, cancellationToken);
            _logger.LogInformation("Notification sent to Service Bus for FileId: {FileId}, Path: {StagedPath}", notification.FileId, notification.StagedFilePath);

            return uniqueFileName;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error saving file '{OriginalFileName}' to storage.", originalFileName);
            throw new IOException($"An error occurred while saving the file '{originalFileName}'.", ex);
        }
    }
}
