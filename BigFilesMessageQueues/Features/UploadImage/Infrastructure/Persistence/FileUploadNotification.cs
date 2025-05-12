namespace BigFilesMessageQueues.Features.UploadImage.Infrastructure.Persistence;

public record FileUploadNotification(
    string FileId,
    string StagedFilePath,
    string OriginalFileName,
    string ContentType,
    DateTime UploadedTimestampUtc);
