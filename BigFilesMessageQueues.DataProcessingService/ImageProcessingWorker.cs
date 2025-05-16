using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;

namespace BigFilesMessageQueues.DataProcessingService;

public class ImageProcessingWorker : BackgroundService
{
    private readonly ILogger<ImageProcessingWorker> _logger;
    private readonly IFileStorageService _fileStorageService;
    private readonly ServiceBusClient _serviceBusClient;
    private readonly ServiceBusProcessor _processor;
    private readonly string _inputQueueName;
    private readonly string _processedFilesPath;

    private const string SequenceProperty = "SequenceId";
    private const string PositionProperty = "Position";
    private const string OriginalFileNameProperty = "OriginalFileName";
    private const string TotalPartsProperty = "Size";

    private readonly ConcurrentDictionary<string, PendingMessage> _pending = new();

    public ImageProcessingWorker(
        ILogger<ImageProcessingWorker> logger,
        IConfiguration configuration,
        ServiceBusClient serviceBusClient,
        IFileStorageService fileStorageService)
    {
        ArgumentNullException.ThrowIfNull(logger);
        _logger = logger;

        ArgumentNullException.ThrowIfNull(configuration);
        _inputQueueName = configuration["AzureServiceBus:ImageProcessingQueueName"];
        if (string.IsNullOrWhiteSpace(_inputQueueName))
        {
            throw new InvalidOperationException("AzureServiceBus:ImageProcessingQueueName shouldn't be null or empty");
        }

        ArgumentNullException.ThrowIfNull(serviceBusClient);
        _serviceBusClient = serviceBusClient;

        ArgumentNullException.ThrowIfNull(fileStorageService);
        _fileStorageService = fileStorageService;

        _processedFilesPath = configuration["FileProcessing:ProcessedFilesPath"];
        if (string.IsNullOrWhiteSpace(_processedFilesPath))
        {
            throw new InvalidOperationException("FileProcessing:ProcessedFilesPath shouldn't be null or empty");
        }

        _fileStorageService.CreateDirectoryIfNotExists(_processedFilesPath);

        var options = new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = false,
            MaxConcurrentCalls = 1
        };

        _processor = _serviceBusClient.CreateProcessor(_inputQueueName, options);
        _processor.ProcessMessageAsync += MessageHandlerAsync;
        _processor.ProcessErrorAsync += ErrorHandlerAsync;
        _fileStorageService = fileStorageService;
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

        if (!message.ApplicationProperties.TryGetValue(SequenceProperty, out object? seqIdObj) || seqIdObj is not string sequenceId || string.IsNullOrEmpty(sequenceId))
        {
            _logger.LogWarning("Message {MessageId} is missing SequenceId or it's not a string. Dead-lettering.", messageId);
            await args.DeadLetterMessageAsync(message, "MissingSequenceId", "SequenceId property is missing or invalid.", args.CancellationToken);
            return;
        }

        if (!message.ApplicationProperties.TryGetValue(PositionProperty, out object? posObj) || posObj is not int position || position < 0)
        {
            _logger.LogWarning("Message {MessageId} for SequenceId {SequenceId} is missing Position or it's invalid. Dead-lettering.", messageId, sequenceId);
            await args.DeadLetterMessageAsync(message, "MissingPosition", "Position property is missing or invalid.", args.CancellationToken);
            return;
        }

        if (!message.ApplicationProperties.TryGetValue(TotalPartsProperty, out object? totalPartsObj) || totalPartsObj is not int currentChunkTotalParts)
        {
            _logger.LogWarning("Message {MessageId} for SequenceId {SequenceId} is missing TotalPartsProperty or it's invalid. Dead-lettering.", messageId, sequenceId);
            await args.DeadLetterMessageAsync(message, "MissingTotalPartsProperty", "TotalPartsProperty property is missing or invalid.", args.CancellationToken);
            return;
        }

        if (!message.ApplicationProperties.TryGetValue(OriginalFileNameProperty, out object? originalNameObj) || originalNameObj is not string originalName || string.IsNullOrEmpty(originalName))
        {
            _logger.LogWarning("Message {MessageId} for SequenceId {SequenceId} is missing OriginalFileNameProperty or it's invalid. Dead-lettering.", messageId, sequenceId);
            await args.DeadLetterMessageAsync(message, "MissingOriginalFileName", "OriginalFileName property is missing or invalid.", args.CancellationToken);
            return;
        }

        var chunk = message.Body.ToArray();

        var pending = _pending.GetOrAdd(sequenceId, id => new PendingMessage
        {
            OriginalFileName = originalName,
            ExpectedParts = currentChunkTotalParts
        });

        if (pending.ExpectedParts != currentChunkTotalParts)
        {
            _logger.LogWarning("Sequence {Seq} size mismatch: expected {Expected}, got {Actual}", sequenceId, pending.ExpectedParts, currentChunkTotalParts);
            _pending.TryRemove(sequenceId, out _);
            await args.DeadLetterMessageAsync(message, "SizeMismatch", null, args.CancellationToken);
            return;
        }

        if (pending.Chunks.TryAdd(position, chunk))
        {
            _logger.LogDebug("Buffered part {Pos}/{Total} for {Seq}", position, currentChunkTotalParts, sequenceId);
        }
        else
        {
            _logger.LogDebug("Duplicate part {Pos} for {Seq} ignored", position, sequenceId);
        }

        await args.CompleteMessageAsync(message, args.CancellationToken);

        if (pending.Chunks.Count == pending.ExpectedParts)
        {
            await JoinChunksAndSaveAsync(sequenceId, pending);
            _pending.TryRemove(sequenceId, out _);
        }
    }

    private async Task JoinChunksAndSaveAsync(string sequenceId, PendingMessage pendingMessage)
    {
        var outputPath = Path.Combine(_processedFilesPath, pendingMessage.OriginalFileName!);
        _logger.LogInformation("Reassembling {Count} parts for Sequence {Seq} into {Path}",
            pendingMessage.Chunks.Count, sequenceId, outputPath);

        var orderedChunks = pendingMessage.Chunks
            .OrderBy(kv => kv.Key)
            .Select(kv => kv.Value);

        await _fileStorageService.SaveFileAsync(outputPath, orderedChunks);
        _logger.LogInformation("Sequence {Seq} written to {File}", sequenceId, pendingMessage.OriginalFileName);
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
