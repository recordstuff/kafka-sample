using KafkaSample.Services;
using Microsoft.AspNetCore.Mvc;

namespace KafkaSample.Controllers;

[ApiController]
[Route("[controller]")]
public class Producer : ControllerBase
{
    private readonly IKafkaProducerService _kafkaProducerService;

    public Producer(IKafkaProducerService kafkaProducerService)
    {
        _kafkaProducerService = kafkaProducerService;
    }

    // GET: api/<Producer>
    [HttpGet]
    public IEnumerable<string> Get()
    {
        return new string[] { "value1", "value2" };
    }


    [HttpPost]
    //public async Task<IActionResult> Post([FromBody] string value)
    //{
    //    await _kafkaProducerService.Send(value);

    //    return Ok();
    //}

    public IActionResult Post([FromBody] string value)
    {
        _kafkaProducerService.Send(value);

        return Ok();
    }
}
