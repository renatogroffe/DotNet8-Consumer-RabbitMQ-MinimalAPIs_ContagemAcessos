using System.Text.Json;
using Oragon.RabbitMQ;
using Oragon.RabbitMQ.Serialization;
using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<ContagemService>();
builder.Services.AddSingleton(
    new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
builder.Services.AddSingleton<IAMQPSerializer, SystemTextJsonAMQPSerializer>();

using var connectionBroker = await new ConnectionFactory
{
    Uri = new Uri(builder.Configuration.GetConnectionString("RabbitMQ")!),
    DispatchConsumersAsync = true
}.CreateConnectionAsync();
builder.Services.AddSingleton(connectionBroker);

builder.Services.MapQueue<ContagemService, ContagemEvent>(config => config
    .WithDispatchInRootScope()    
    .WithAdapter((svc, msg) => svc.ProcessAsync(msg))
    .WithQueueName(builder.Configuration["QueueName"])
    .WithPrefetchCount(1)
);

var app = builder.Build();

app.UseHttpsRedirection();

// Endpoint para Health Check
app.MapGet("/status", () => TypedResults.Ok());

app.Logger.LogInformation("Iniciando aplicacao...");
app.Run();

public class ContagemEvent
{
    public long ValorAtual { get; set; }
    public string? Producer { get; set; }
    public string? Kernel { get; set; }
    public string? Framework { get; set; }
    public string? Mensagem { get; set; }
}

public class ContagemService
{
    private readonly ILogger<ContagemService> _logger;
    private readonly int _intervalConsumer;

    public ContagemService(IConfiguration configuration, ILogger<ContagemService> logger)
    {
        _logger = logger;
        _intervalConsumer = Convert.ToInt32(configuration["Interval"]);
    }
    public async Task ProcessAsync(ContagemEvent eventData)
    {
        _logger.LogInformation($"Aguardando {_intervalConsumer}ms para concluir processamento...");
        _logger.LogInformation($"Valor atual: {eventData.ValorAtual} | Producer: {eventData.Producer} | " + 
            $"Kernel: {eventData.Kernel} | Framework: {eventData.Framework} | Mensagem: {eventData.Mensagem}");
        await Task.Delay(_intervalConsumer);
    }
}