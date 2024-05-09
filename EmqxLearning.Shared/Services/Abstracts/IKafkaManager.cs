using Confluent.Kafka;
namespace EmqxLearning.Shared.Services.Abstracts;

public interface IKafkaManager
{
    Task<(IProducer<string, string>, string err)> StartProducerAsync(string topicName, CancellationToken token = default);
    Task<(IConsumer<string, string>, string err)> StartConsumerAsync(string topicName, CancellationToken token = default);

    Task<(bool success, string err)> ProduceMessageAsync(string topicName, string[] messages, CancellationToken token = default);
    Task<(bool success, string err)> ProduceMessageAsync(string topicName, string messages, CancellationToken token = default);
    Task ACK(string topicName, long offset, CancellationToken token = default);
    Task ReleaseProducersAsync(string[] topicNames);
    Task ReleaseConsumersAsync(string[] topicNames);
}