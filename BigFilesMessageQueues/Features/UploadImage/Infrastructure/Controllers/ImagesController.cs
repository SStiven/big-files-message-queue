using Microsoft.AspNetCore.Mvc;

namespace BigFilesMessageQueues.Features.UploadImage.Infrastructure.Controllers;

[ApiController]
[Route("api/images")]
public class ImagesController : ControllerBase
{
    private readonly ILogger<ImagesController> _logger;
    private readonly UploadImageHandler _handler;

    public ImagesController(
        ILogger<ImagesController> logger,
        UploadImageHandler handler)
    {
        _logger = logger;
        _handler = handler;
    }

    [HttpPost]
    [ProducesResponseType(StatusCodes.Status202Accepted)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status500InternalServerError)]
    public async Task<IActionResult> UploadImage(IFormFile file)
    {
        if (file == null || file.Length == 0)
        {
            _logger.LogWarning("Upload attempt with no file or empty file");
            return BadRequest("No file or empty file uploaded");
        }

        using Stream stream = file.OpenReadStream();
        var command = new UploadImageCommand(
            stream,
            file.FileName,
            file.ContentType
        );

        var result = await _handler.ExecuteAsync(command);
        if (result.IsSuccess)
        {
            return Accepted(new { FileId = result.FileId, Message = "File upload was a success and queued for processing" });
        }

        _logger.LogWarning("File upload processing failed");
        return BadRequest();
    }
}