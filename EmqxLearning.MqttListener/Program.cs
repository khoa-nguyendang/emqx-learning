
using EmqxLearning.Shared.Services;
using EmqxLearning.Shared.Services.Abstracts;
using Serilog;
namespace EmqxLearning.MqttListener
{

    public class Program
    {
        public static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration().WriteTo.Console().CreateLogger();
            var builder = WebApplication.CreateBuilder(args);

            // Add services to the container.
            builder.Services.AddAuthorization();

            // Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
            builder.Services.AddEndpointsApiExplorer();
            builder.Services.AddSwaggerGen();
            builder.Services.AddLogging();
            builder.Services.AddSerilog();
            builder.Services.AddSingleton<IKafkaManager, KafkaManager>();
            var resilienceSettings = builder.Configuration.GetSection("ResilienceSettings");
            builder.Services.AddHostedService<Worker>();
            var app = builder.Build();

            // Configure the HTTP request pipeline.
            if (app.Environment.IsDevelopment())
            {
                app.UseSwagger();
                app.UseSwaggerUI();
            }

            app.UseHttpsRedirection();

            app.UseAuthorization();
            Log.Information("Starting MqttListener application");
            app.Run();
        }
    }
}
