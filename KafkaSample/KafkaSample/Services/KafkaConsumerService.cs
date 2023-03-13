using Confluent.Kafka;

namespace KafkaSample.Services;

public class KafkaConsumerService : BackgroundService, IHostedService
{
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly ConsumerConfig _config;
    private readonly IConsumer<Ignore, string> _consumer;

    public KafkaConsumerService(ILogger<KafkaConsumerService> logger)
    {
        _logger = logger;
        
        _config = new ConsumerConfig
        {
            BootstrapServers = "kafka:29092",
            GroupId = "kafka-sample",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };



        _consumer = new ConsumerBuilder<Ignore, string>(_config).Build();
        _consumer.Subscribe("test-topic");
    }
    
    public override Task StopAsync(CancellationToken cancellationToken)
    {
        _consumer.Close();
        _consumer.Dispose();

        return base.StopAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        // give time for the kafka container to auto create the test-topic

        Thread.Sleep(3000);

        // todo use health checks in docker compose?

        _logger.LogInformation($"Blocking on background thread for test-topic.");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var result = _consumer.Consume(stoppingToken);
                _logger.LogInformation($"Received {result.Message.Value}.");
            }
        }
        catch (OperationCanceledException)
        {
            await StopAsync(stoppingToken);
        }
    }
}
