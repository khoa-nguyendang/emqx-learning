
using EmqxLearning.KafkaConsumer.Services;
using EmqxLearning.KafkaConsumer.Services.Abstracts;
using EmqxLearning.Shared.Exceptions;
using EmqxLearning.Shared.Extensions;
using EmqxLearning.Shared.Services;
using EmqxLearning.Shared.Services.Abstracts;
using Polly.Registry;
using Serilog;

namespace EmqxLearning.KafkaConsumer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration().WriteTo.Console().CreateLogger();
            var builder = WebApplication.CreateBuilder(args);

            // Add services to the container.

            builder.Services.AddControllers();
            // Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
            builder.Services.AddEndpointsApiExplorer();
            builder.Services.AddSwaggerGen();
            builder.Services.AddLogging();
            builder.Services.AddSerilog();
            ConfigServices(builder.Services, builder.Configuration);
            var app = builder.Build();

            // Configure the HTTP request pipeline.
            if (app.Environment.IsDevelopment())
            {
                app.UseSwagger();
                app.UseSwaggerUI();
            }

            app.UseHttpsRedirection();

            app.UseAuthorization();


            app.MapControllers();
            Log.Information("Starting KafkaConsumer application");
            app.Run();
        }



        private static void ConfigServices(IServiceCollection services, IConfiguration configuration)
        {
            services.AddSingleton<IKafkaManager, KafkaManager>();
            services.AddTransient<IngestionService>();
            services.AddTransient<BatchIngestionService>();
            services.AddSingleton<IIngestionService>(provider =>
            {
                var configuration = provider.GetRequiredService<IConfiguration>();
                var useBatchInsert = configuration.GetValue<bool>("BatchSettings:Enabled");
                return useBatchInsert
                    ? provider.GetRequiredService<BatchIngestionService>()
                    : provider.GetRequiredService<IngestionService>();
            });
           

            var resilienceSettings = configuration.GetSection("ResilienceSettings");
            SetupResilience(services, resilienceSettings);

            services.AddHostedService<Worker>(provider =>
            {
                var logger = provider.GetRequiredService<ILogger<Worker>>();
                var cf = provider.GetRequiredService<IConfiguration>();
                var km = provider.GetRequiredService<IKafkaManager>();
                var ig = provider.GetRequiredService<IIngestionService>();
                var rp = provider.GetRequiredService<ResiliencePipelineProvider<string>>();
                return new Worker(logger, cf, km, ig, rp);
            });
        }

        private static IServiceCollection SetupResilience(IServiceCollection services, IConfiguration resilienceSettings)
        {
            const string ConnectionErrorsKey = Constants.ResiliencePipelines.ConnectionErrors;
            const string TransientErrorsKey = Constants.ResiliencePipelines.TransientErrors;
            return services.AddSingleton<ResiliencePipelineProvider<string>>(provider =>
            {
                var registry = new ResiliencePipelineRegistry<string>();
                registry.TryAddBuilder(ConnectionErrorsKey, (builder, _) =>
                {
                    builder.AddDefaultRetry(
                        retryAttempts: resilienceSettings.GetValue<int?>($"{ConnectionErrorsKey}:RetryAttempts") ?? int.MaxValue,
                        delaySecs: resilienceSettings.GetValue<int>($"{ConnectionErrorsKey}:DelaySecs")
                    );
                });
                registry.TryAddBuilder(TransientErrorsKey, (builder, _) =>
                {
                    builder.AddDefaultRetry(
                        retryAttempts: resilienceSettings.GetValue<int>($"{TransientErrorsKey}:RetryAttempts"),
                        delaySecs: resilienceSettings.GetValue<int>($"{TransientErrorsKey}:DelaySecs"),
                        shouldHandle: (ex) => new ValueTask<bool>(ex.Outcome.Exception != null && ex.Outcome.Exception is not CircuitOpenException)
                    );
                });
                return registry;
            });
        }
    }
}
