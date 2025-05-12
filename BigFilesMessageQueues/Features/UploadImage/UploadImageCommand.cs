namespace BigFilesMessageQueues.Features.UploadImage;

public record UploadImageCommand(Stream FileStream, string OriginalFileName, string ContentType);