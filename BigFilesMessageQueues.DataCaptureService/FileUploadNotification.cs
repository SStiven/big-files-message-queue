namespace BigFilesMessageQueues.DataCaptureService;

public record FileUploadNotification(
    string FileId,
    string StagedFilePath,
    string OriginalFileName,
    string ContentType,
    DateTime UploadedTimestampUtc);
