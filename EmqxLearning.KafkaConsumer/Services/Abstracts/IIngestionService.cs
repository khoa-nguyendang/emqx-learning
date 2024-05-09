
using Confluent.Kafka;

namespace EmqxLearning.KafkaConsumer.Services.Abstracts;

public interface IIngestionService
{
    void Configure(Func<Task> reconnectConsumer);
    Task HandleMessage(ConsumeResult<string, string> consumeResult, CancellationToken cancellationToken);
}