using Microsoft.AspNetCore.Mvc;

namespace BigFilesMessageQueues.Controllers;

[ApiController]
[Route("big-file")]
public class BigFileController : ControllerBase
{
    private readonly ILogger<BigFileController> _logger;

    public BigFileController(ILogger<BigFileController> logger)
    {
        _logger = logger;
    }

    [HttpGet]
    public IActionResult Get()
    {
        return Ok("ok");
    }
}
