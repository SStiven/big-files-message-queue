namespace BigFilesMessageQueues.DataProcessingService;

public class LocalStorageService : IFileStorageService
{
    private readonly ILogger<LocalStorageService> _logger;
    private readonly string _processedFilesPath;

    public LocalStorageService(ILogger<LocalStorageService> logger, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(logger);
        _logger = logger;

        ArgumentNullException.ThrowIfNull(configuration);
        _processedFilesPath = configuration["FileProcessing:ProcessedFilesPath"];
        if (string.IsNullOrWhiteSpace(_processedFilesPath))
        {
            throw new InvalidOperationException("FileProcessing:ProcessedFilesPath shouldn't be null or empty");
        }
    }

    public void CreateDirectoryIfNotExists(string? path)
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

    public async Task SaveFileAsync(string path, IEnumerable<byte[]> chunks, CancellationToken cancellationToken = default)
    {
        using var fs = File.Create(path);
        foreach (var chunk in chunks)
        {
            await fs.WriteAsync(chunk, 0, chunk.Length, cancellationToken);
        }
        await fs.FlushAsync(cancellationToken);
        _logger.LogInformation("Saved file to {Path}", path);
    }
}
