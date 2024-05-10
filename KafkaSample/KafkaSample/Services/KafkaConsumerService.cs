using Confluent.Kafka;
using Confluent.Kafka.Admin;

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
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        _consumer.Close();
        _consumer.Dispose();

        return base.StopAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = "kafka:29092"
        };

        using (var adminClient = new AdminClientBuilder(config).Build())
        {
            try
            {
                await adminClient.CreateTopicsAsync(new List<TopicSpecification>
                {
                    new TopicSpecification
                    {
                        Name = "test-topic",
                        NumPartitions = 1,
                        ReplicationFactor = 1
                    }
                });
            }
            catch (CreateTopicsException e)
            {
                Console.WriteLine($"An error occurred creating the topic: {e.Results[0].Error.Reason}");
            }
        }

        _consumer.Subscribe("test-topic");

        await Task.Yield();

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
