namespace BigFilesMessageQueues.DataProcessingService;

public interface IFileStorageService
{
    void CreateDirectoryIfNotExists(string? path);
    Task SaveFileAsync(string path, IEnumerable<byte[]> chunks, CancellationToken cancellationToken = default);
}
