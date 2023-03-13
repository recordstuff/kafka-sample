using Confluent.Kafka;

namespace KafkaSample.Services;

public class KafkaProducerService : IKafkaProducerService
{
    private readonly ILogger<KafkaProducerService> _logger;
    private readonly ProducerConfig _config;
    private readonly IProducer<Null, string> _producer;

    public KafkaProducerService(ILogger<KafkaProducerService> logger)
    {
        _logger = logger;
        _config = new ProducerConfig
        {
            BootstrapServers = "kafka:29092",
            EnableDeliveryReports = true,
            EnableIdempotence = true
        };

        _producer = new ProducerBuilder<Null, string>(_config).Build();
        _logger = logger;
    }

    public void Send(string  message)
    {
        _producer.Produce("test-topic",  new Message<Null, string> 
        {
            Value = message 
        },
        (DeliveryReport<Null, string> deliveryReport) =>
        {
            var error = deliveryReport.Error;
        });
        //_producer.Poll(TimeSpan.FromSeconds(0));
        _producer.Flush();
    }

    public void Dispose()
    {
        _producer.Flush();
        _producer.Dispose();
    }
}
