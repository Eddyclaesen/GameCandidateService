using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace GameCandidateService
{
    public sealed class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("=== GameCandidateService gestart ===");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    _logger.LogInformation("Nieuwe iteratie gestart...");

                    // voorlopig: roep je bestaande flow aan (die zetten we straks om)
                    await Program.RunOnceAsync(_logger, stoppingToken);

                    _logger.LogInformation("Iteratie klaar. Wachten 5 minuten...");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Fout in hoofdprocedure");
                }

                await Task.Delay(TimeSpan.FromMinutes(5), stoppingToken);
            }
        }
    }
}
