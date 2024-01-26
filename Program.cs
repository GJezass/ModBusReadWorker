using BBox_ModBusWorker;
using Microsoft.Extensions.Configuration;

public class Program
{
    public static IConfiguration? Configuration { get; set; }

    public static void Main(string[] args)
    {
        IHost host = Host.CreateDefaultBuilder(args)
        .ConfigureAppConfiguration((hostingContext, config) =>
        {
            config.SetBasePath(Directory.GetCurrentDirectory());
            config.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
            config.AddEnvironmentVariables();
        })
        .ConfigureServices((hostContext, services) =>
        {
            Configuration = hostContext.Configuration;
            services.AddHostedService<Worker>();
        })
        .Build();

        host.Run();
    }
}

