namespace BigFilesMessageQueues.DataProcessingService;

public class PendingMessage
{
    public SortedDictionary<int, byte[]> Chunks { get; } = new();
    public int ExpectedParts { get; set; }
    public string? OriginalFileName { get; set; }
}