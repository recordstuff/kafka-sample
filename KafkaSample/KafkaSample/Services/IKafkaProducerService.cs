namespace KafkaSample.Services;

public interface IKafkaProducerService : IDisposable
{
    public void Send(string message);
}
