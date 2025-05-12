using BigFilesMessageQueues.Features.UploadImage.Services;

namespace BigFilesMessageQueues.Features.UploadImage;

public class UploadImageHandler
{
    private readonly ILogger<UploadImageHandler> _logger;
    private readonly IFileStorageService _fileStorageService;

    public UploadImageHandler(
        ILogger<UploadImageHandler> logger,
        IFileStorageService fileStorageService)
    {
        _logger = logger;
        _fileStorageService = fileStorageService;
    }

    public async Task<UploadImageResult> ExecuteAsync(UploadImageCommand command, CancellationToken cancellationToken = default)
    {
        if (command.FileStream == null || command.FileStream.Length == 0)
        {
            throw new ArgumentException("File stream cannot be null or empty", nameof(command.FileStream));
        }

        var originalFileName = command.OriginalFileName;
        if (string.IsNullOrWhiteSpace(originalFileName))
        {
            throw new ArgumentException("Original file name must be provided", nameof(command.OriginalFileName));
        }

        var extension = Path.GetExtension(originalFileName);

        var fileName = await _fileStorageService.SaveFileAsync(command.FileStream, originalFileName, command.ContentType, cancellationToken);

        _logger.LogInformation($"File uploaded");

        return new UploadImageResult(fileName, true);
    }
}
