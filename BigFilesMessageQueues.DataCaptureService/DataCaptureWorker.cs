using System.Text.Json;
using System.Threading;
using Azure.Messaging.ServiceBus;

namespace BigFilesMessageQueues.DataCaptureService;

public class DataCaptureWorker : BackgroundService
{
    private readonly ILogger<DataCaptureWorker> _logger;
    private readonly string _stagingAreaPath;
    private readonly ServiceBusClient _serviceBusClient;
    private readonly ServiceBusProcessor _notificationProcessor;
    private readonly ServiceBusSender _processingQueueSender;
    private readonly string _processingQueueName;
    private readonly long _maxFileSizeBeforeChunkingInBytes;
    private readonly long _chunkSizeInBytes;

    private const string SequenceIdProperty = "SequenceId";
    private const string PositionProperty = "Position";
    private const string IsLastChunkProperty = "IsLastChunk";
    private const string OriginalFileNameProperty = "OriginalFileName";
    private const string ContentTypeProperty = "ContentType";

    public DataCaptureWorker(
        ILogger<DataCaptureWorker> logger,
        IConfiguration configuration,
        ServiceBusClient serviceBusClient)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        _logger = logger;
        _serviceBusClient = serviceBusClient;

        var notificationQueueName = configuration["AzureServiceBus:UploadNotificationQueueName"];
        if (string.IsNullOrEmpty(notificationQueueName))
        {
            _logger.LogError("AzureServiceBus:UploadNotificationQueueName is not configured. Service cannot start.");
            throw new InvalidOperationException("UploadNotificationQueueName is not configured");
        }

        _processingQueueName = configuration["AzureServiceBus:ImageProcessingQueueName"];
        if (string.IsNullOrEmpty(_processingQueueName))
        {
            _logger.LogError("AzureServiceBus:ImageProcessingQueueName is not configured. Service cannot start");
            throw new InvalidOperationException("ImageProcessingQueueName is not configured");
        }

        _stagingAreaPath = configuration["FileUploads:StagingAreaPath"];
        if (string.IsNullOrEmpty(_stagingAreaPath))
        {
            throw new InvalidOperationException("Staging area path cannot be null");
        }

        if (!long.TryParse(configuration["FileUploads:MaxFileSizeBeforeChunkingInKB"], out long maxKb) || maxKb <= 0)
        {
            throw new InvalidOperationException("FileUploads:MaxFileSizeBeforeChunkingInKB is not valid");
        }

        if (!long.TryParse(configuration["FileUploads:ChunkSizeInKB"], out long chunkInKb) || chunkInKb <= 0)
        {
            throw new InvalidOperationException("FileUploads:ChunkSizeInKB is not valid");
        }

        _maxFileSizeBeforeChunkingInBytes = maxKb * 1024;
        _chunkSizeInBytes = chunkInKb * 1024;

        _notificationProcessor = _serviceBusClient.CreateProcessor(notificationQueueName);
        _processingQueueSender = _serviceBusClient.CreateSender(_processingQueueName);

        _notificationProcessor.ProcessMessageAsync += MessageHandlerAsync;
        _notificationProcessor.ProcessErrorAsync += ErrorHandlerAsync;

    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _notificationProcessor.StartProcessingAsync(stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {

            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
            }
            await Task.Delay(3000, stoppingToken);
        }

        await _notificationProcessor.StopProcessingAsync(CancellationToken.None);
        _logger.LogInformation("Notification message processor finished");
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping processor...");
        await _notificationProcessor.StopProcessingAsync(cancellationToken).ConfigureAwait(false);
        await _notificationProcessor.DisposeAsync().ConfigureAwait(false);
        _logger.LogInformation("Processor disposed.");
        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }

    private Task ErrorHandlerAsync(ProcessErrorEventArgs args)
    {
        _logger.LogError($"Error: {args.Exception.Message}");
        return Task.CompletedTask;
    }

    private async Task MessageHandlerAsync(ProcessMessageEventArgs args)
    {
        string messageId = args.Message.MessageId ?? "N/A";
        _logger.LogInformation("Received notification message: SequenceNumber:{SequenceNumber}, MessageId:{MessageId}", args.Message.SequenceNumber, messageId);

        var body = args.Message.Body.ToString();
        FileUploadNotification? notification = null;

        try
        {
            notification = JsonSerializer.Deserialize<FileUploadNotification>(body);
            if (notification == null)
            {
                _logger.LogWarning("Failed to deserialize notification. Sending it to Dead letter. MessageId: {MessageId}", args.Message.MessageId ?? "N/A");
                await args.DeadLetterMessageAsync(args.Message, "DeserializationError", "Could not deserialize notification body.", args.CancellationToken);
                return;
            }

            _logger.LogInformation("Processing notification for FileId: {FileId}, OriginalName: {FileName}", notification.FileId, notification.OriginalFileName);

            var fullPath = Path.Combine(_stagingAreaPath, notification.StagedFilePath);
            if (!File.Exists(fullPath))
            {
                _logger.LogError("Staged file not found at path: {FilePath}. Dead-lettering notification. FileId: {FileId}", fullPath, notification.FileId);
                await args.DeadLetterMessageAsync(args.Message, "FileNotFound", $"Staged file not found at {fullPath}", args.CancellationToken);
                return;
            }

            var fileInfo = new FileInfo(fullPath);

            await SendFileMessageAsync(notification, fullPath, fileInfo.Length, args.CancellationToken);

            await args.CompleteMessageAsync(args.Message, args.CancellationToken);
            _logger.LogInformation("Successfully processed and completed notification for FileId: {FileId}", notification.FileId);

        }
        catch (JsonException jsonException)
        {
            _logger.LogError(jsonException, "Json Deserialization error for notification message. Dead-lettering. MessageId: {MessageId}", args.Message.MessageId ?? "N/A");
            await args.DeadLetterMessageAsync(args.Message, "BadNotificationFormat", jsonException.Message, args.CancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error processing notification FileId: {FileId}. Dead-lettering.", notification?.FileId ?? "N/A");
            await args.DeadLetterMessageAsync(args.Message, "ProcessingError", ex.Message, args.CancellationToken);
        }

        _logger.LogInformation($"MessageHandlerAsync: {body}");
    }

    private async Task SendFileMessageAsync(FileUploadNotification notification, string fullPath, long fileLengthInBytes, CancellationToken cancellationToken)
    {
        _logger.LogInformation($"File is smaller than chunking threshold: {_maxFileSizeBeforeChunkingInBytes} Bytes. Sending as single message.", notification.StagedFilePath);

        byte[] fileContent = await File.ReadAllBytesAsync(fullPath, cancellationToken);
        var message = new ServiceBusMessage(fileContent);

        message.ApplicationProperties[SequenceIdProperty] = notification.FileId;
        message.ApplicationProperties[PositionProperty] = 1;
        message.ApplicationProperties[IsLastChunkProperty] = true;

        message.ApplicationProperties[OriginalFileNameProperty] = notification.OriginalFileName;
        message.ApplicationProperties[ContentTypeProperty] = notification.ContentType;

        message.MessageId = $"{notification.FileId}-1";
        message.ContentType = notification.ContentType;

        await _processingQueueSender.SendMessageAsync(message, cancellationToken);
        _logger.LogInformation("Sent single message for FileId: {FileId} to queue '{QueueName}'", notification.FileId, _processingQueueName);
    }
}
