using Azure.Messaging.ServiceBus;

namespace BigFilesMessageQueues.DataProcessingService;

public class ImageProcessingWorker : BackgroundService
{
    private readonly ILogger<ImageProcessingWorker> _logger;
    private readonly ServiceBusClient _serviceBusClient;
    private readonly ServiceBusProcessor _processor;
    private readonly string _inputQueueName;
    private readonly string _processedFilesPath;

    private const string SequenceIdProperty = "SequenceId";
    private const string PositionProperty = "Position";
    private const string IsLastChunkProperty = "IsLastChunk";
    private const string OriginalFileNameProperty = "OriginalFileName";
    private const string ContentTypeProperty = "ContentType";


    public ImageProcessingWorker(
        ILogger<ImageProcessingWorker> logger,
        IConfiguration configuration,
        ServiceBusClient serviceBusClient)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(serviceBusClient);

        _logger = logger;
        _serviceBusClient = serviceBusClient;

        _inputQueueName = configuration["AzureServiceBus:ImageProcessingQueueName"];
        if (string.IsNullOrWhiteSpace(_inputQueueName))
        {
            throw new InvalidOperationException("AzureServiceBus:ImageProcessingQueueName shouldn't be null or empty");
        }

        _processedFilesPath = configuration["FileProcessing:ProcessedFilesPath"];
        if (string.IsNullOrWhiteSpace(_processedFilesPath))
        {
            throw new InvalidOperationException("FileProcessing:ProcessedFilesPath shouldn't be null or empty");
        }

        CreateDirectoryExists(_processedFilesPath);

        var options = new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = false,
            MaxConcurrentCalls = 1
        };

        _processor = _serviceBusClient.CreateProcessor(_inputQueueName, options);
        _processor.ProcessMessageAsync += MessageHandlerAsync;
        _processor.ProcessErrorAsync += ErrorHandlerAsync;

    }

    private Task ErrorHandlerAsync(ProcessErrorEventArgs args)
    {
        _logger.LogError($"Error: {args.Exception.Message}");
        return Task.CompletedTask;
    }

    private async Task MessageHandlerAsync(ProcessMessageEventArgs args)
    {
        var message = args.Message;
        string messageId = message.MessageId ?? "N/A";

        if (!message.ApplicationProperties.TryGetValue(SequenceIdProperty, out object? seqIdObj) || seqIdObj is not string sequenceId || string.IsNullOrEmpty(sequenceId))
        {
            _logger.LogWarning("Message {MessageId} is missing SequenceId or it's not a string. Dead-lettering.", messageId);
            await args.DeadLetterMessageAsync(message, "MissingSequenceId", "SequenceId property is missing or invalid.", args.CancellationToken);
            return;
        }

        if (!message.ApplicationProperties.TryGetValue(PositionProperty, out object? posObj) || posObj is not int position || position <= 0)
        {
            _logger.LogWarning("Message {MessageId} for SequenceId {SequenceId} is missing Position or it's invalid. Dead-lettering.", messageId, sequenceId);
            await args.DeadLetterMessageAsync(message, "MissingPosition", "Position property is missing or invalid.", args.CancellationToken);
            return;
        }

        if (!message.ApplicationProperties.TryGetValue(IsLastChunkProperty, out object? lastObj) || lastObj is not bool isLastChunk)
        {
            _logger.LogWarning("Message {MessageId} for SequenceId {SequenceId} is missing IsLastChunk or it's invalid. Dead-lettering.", messageId, sequenceId);
            await args.DeadLetterMessageAsync(message, "MissingIsLastChunk", "IsLastChunk property is missing or invalid.", args.CancellationToken);
            return;
        }

        message.ApplicationProperties.TryGetValue(OriginalFileNameProperty, out object? originalImageName);

        byte[] fileBytes = message.Body.ToArray();
        var originalFileName = originalImageName as string ?? $"{sequenceId}.bin";
        var outputPath = Path.Combine(_processedFilesPath, originalFileName);
        await File.WriteAllBytesAsync(outputPath, fileBytes, args.CancellationToken);
        _logger.LogInformation("Saved image {File} ({Size} bytes) to {Path}", originalFileName, fileBytes.Length, outputPath);
        await args.CompleteMessageAsync(message, args.CancellationToken);
    }

    private void CreateDirectoryExists(string? path)
    {
        try
        {
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
                _logger.LogInformation("Created directory: {Path}", path);
            }
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Failed to ensure directory exists: {Path}. Service cannot save processed files.", path);
            throw;
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting image processing");

        await _processor.StartProcessingAsync(stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
            }
            await Task.Delay(3000, stoppingToken);
        }

        _logger.LogInformation("Stopping file processing message processor...");
        await _processor.StopProcessingAsync(CancellationToken.None);
        _logger.LogInformation("File processing message processor stopped.");

    }
}
