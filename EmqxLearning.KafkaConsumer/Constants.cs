namespace EmqxLearning.KafkaConsumer;

public static class Constants
{
    public static class ResiliencePipelines
    {
        public const string ConnectionErrors = nameof(ConnectionErrors);
        public const string TransientErrors = nameof(TransientErrors);
    }
}