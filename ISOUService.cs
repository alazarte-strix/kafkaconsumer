namespace KafkaConsumer
{
    public interface ISOUService
    {
        Task<string> GetTokenAsync();

        Task SendSOUAsync(string accountId);

        Task RetryFailedMessagesAsync();
    }

}
