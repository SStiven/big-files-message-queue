namespace BigFilesMessageQueues.Features.UploadImage.Services;

public interface IFileStorageService
{
    Task<string> SaveFileAsync(Stream fileStream, string originalFileName, string contentType, CancellationToken cancellationToken = default);
}
