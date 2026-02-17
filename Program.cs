using System;
using System.Collections.Generic;
using System.Data;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics;
using System.IO;
using Serilog;
using Serilog.Events;
using ILogger = Microsoft.Extensions.Logging.ILogger;

namespace GameCandidateService
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            var logRoot = @"C:\Services\GameCandidateService\logs";
            Directory.CreateDirectory(logRoot);

            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Information()
                .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
                .MinimumLevel.Override("System", LogEventLevel.Warning)
                .WriteTo.Console(
                    outputTemplate: "{Timestamp:HH:mm:ss} [{Level:u3}] {Message:lj}{NewLine}{Exception}"
                )
                .WriteTo.File(
                    Path.Combine(logRoot, "service-.log"),
                    rollingInterval: RollingInterval.Day,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} [{Level:u3}] {Message:lj}{NewLine}{Exception}"
                )
                .CreateLogger();

            var builder = Host.CreateDefaultBuilder(args)
                .UseSerilog()
                .ConfigureServices(services =>
                {
                    services.AddHostedService<Worker>();
                });

            if (!Environment.UserInteractive)
                builder.UseWindowsService();

            try
            {
                await builder.Build().RunAsync();
            }
            finally
            {
                Log.CloseAndFlush();
            }
        }

        // Debug mode: wordt gecontroleerd via environment variable "DEBUG_MODE=true"
        private static bool IsDebugMode =>
            Environment.GetEnvironmentVariable("DEBUG_MODE")?.ToLower() == "true";

        private static void WaitForUserIfDebug(string message = "Druk op ENTER om verder te gaan...")
        {
            if (IsDebugMode)
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"\n⏸️  [DEBUG PAUSE] {message}");
                Console.ResetColor();
                Console.ReadLine();
            }
        }

        public static async Task RunOnceAsync(ILogger logger, CancellationToken ct)
        {
            logger.LogInformation("Iteratie gestart.");

            if (IsDebugMode)
            {
                logger.LogWarning("🐛 DEBUG MODE ACTIEF - Service pauzeert tussen stappen");
            }

            WaitForUserIfDebug("Start met Taak 1 (Kandidaten verzenden naar Azure)");

            // 1) jouw eerste blok: ophalen candidates + POST + insert SentCandidatesToAzure
            await SendCandidatesToAzureAsync(logger, ct);

            logger.LogInformation("Taak 1 klaar. Start taak 2...");
            WaitForUserIfDebug("Taak 1 voltooid. Controleer nu SentCandidatesToAzure tabel en start Taak 2 (Voltooiing controleren)");

            await UpdateCompletionStatusesAsync(logger, ct);

            logger.LogInformation("Start taak 3...");
            WaitForUserIfDebug("Taak 2 voltooid. Controleer nu Completed velden en start Taak 3 (Replicatie)");

            await ReplicateResultsAsync(logger, ct);

            logger.LogInformation("Iteratie afgerond.");
            WaitForUserIfDebug("Alle taken voltooid. Druk ENTER voor volgende iteratie (of CTRL+C om te stoppen)");
        }

        public static async Task SendCandidatesToAzureAsync(ILogger logger, CancellationToken ct)
        {
            logger.LogInformation("Start taak 1: kandidaten ophalen en verzenden...");

            // Connection string komt uit env var (zoals we eerder deden)
            var config = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: false)
                .AddEnvironmentVariables()
                .Build();

            string quintessenceDbConn = config.GetConnectionString("DefaultConnection");
            string gamifiedReadConn = config.GetConnectionString("GamifiedRead");

            using var connection = new SqlConnection(quintessenceDbConn);
            using var command = new SqlCommand("dbo.game_GetCandidates", connection)
            {
                CommandType = CommandType.StoredProcedure
            };

            await connection.OpenAsync(ct);

            using var reader = await command.ExecuteReaderAsync(ct);

            int count = 0;

            while (await reader.ReadAsync(ct))
            {
                ct.ThrowIfCancellationRequested();

                var projectCandidateId = reader.GetGuid(0);
                var candidateUserId = reader.GetGuid(1);
                var language = reader.GetString(2);
                var manifestId = reader.GetInt32(3);
                var appointmentDate = reader.GetDateTime(4);
                var assessmentId = reader.GetString(5);

                logger.LogInformation("Kandidaat {Count}: {CandidateUserId}", ++count, candidateUserId);

                // Token ophalen
                var token = await GetAccessTokenAsync();
                if (string.IsNullOrEmpty(token))
                {
                    logger.LogWarning("Token ophalen mislukt. Kandidaat overslaan.");
                    continue;
                }

                // JSON body
                var jsonBody = new
                {
                    assessmentId = assessmentId,
                    language = language,
                    startTime = DateTime.UtcNow.Date.ToString("yyyy-MM-dd"),
                    endTime = appointmentDate.AddDays(100).ToString("yyyy-MM-dd")
                };

                var apiUrl = $"https://gamifiedmanagement.quintessence.be/api/assessments/{candidateUserId}";

                using var httpClient = new HttpClient();
                httpClient.DefaultRequestHeaders.Authorization =
                    new AuthenticationHeaderValue("Bearer", token);

                var content = new StringContent(
                    JsonSerializer.Serialize(jsonBody),
                    Encoding.UTF8,
                    "application/json"
                );

                var response = await httpClient.PostAsync(apiUrl, content, ct);

                if (!response.IsSuccessStatusCode)
                {
                    var error = await response.Content.ReadAsStringAsync(ct);
                    logger.LogError("POST faalde: {Status} - {Error}", response.StatusCode, error);
                    continue;
                }

                logger.LogInformation("POST succesvol.");
                WaitForUserIfDebug($"POST succesvol voor kandidaat {candidateUserId}. Ga nu Uid ophalen uit Azure...");

                // Uid ophalen uit Azure Gamified database
                Guid? azureUid = null;

                try
                {
                    using var uidConn = new SqlConnection(gamifiedReadConn);
                    await uidConn.OpenAsync(ct);

                    var uidCmd = new SqlCommand(@"
                        SELECT TOP 1 Uid
                        FROM [Quintessence.GamifiedAssessment].[UserAssessmentManifests]
                        WHERE UserId = @UserId
                        ORDER BY Id DESC
                    ", uidConn);

                    uidCmd.Parameters.AddWithValue("@UserId", candidateUserId);

                    var result = await uidCmd.ExecuteScalarAsync(ct);

                    if (result != null && Guid.TryParse(result.ToString(), out Guid parsedUid))
                    {
                        azureUid = parsedUid;
                        logger.LogInformation("Azure Uid opgehaald: {Uid}", azureUid);
                        WaitForUserIfDebug($"Uid succesvol opgehaald: {azureUid}. Ga nu inserten in SentCandidatesToAzure...");
                    }
                    else
                    {
                        logger.LogWarning("Geen Uid gevonden in Azure voor UserId {UserId}.", candidateUserId);
                        WaitForUserIfDebug("WAARSCHUWING: Geen Uid gevonden! Controleer of de POST wel een record heeft aangemaakt in Azure.");
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Fout bij ophalen van Azure Uid voor UserId {UserId}", candidateUserId);
                }

                // Insert in SentCandidatesToAzure
                try
                {
                    using var insertConn = new SqlConnection(quintessenceDbConn);
                    await insertConn.OpenAsync(ct);

                    var insertLog = new SqlCommand(@"
                        INSERT INTO dbo.SentCandidatesToAzure 
                        (ProjectCandidateId, CandidateUserId, Language, ManifestId, AppointmentDate, AssessmentId, SendToAzure, Completed, Uid)
                        VALUES (@ProjectCandidateId, @CandidateUserId, @Language, @ManifestId, @AppointmentDate, @AssessmentId, GETDATE(), 0, @Uid)
                    ", insertConn);

                    insertLog.Parameters.AddWithValue("@ProjectCandidateId", projectCandidateId);
                    insertLog.Parameters.AddWithValue("@CandidateUserId", candidateUserId);
                    insertLog.Parameters.AddWithValue("@Language", language);
                    insertLog.Parameters.AddWithValue("@ManifestId", manifestId);
                    insertLog.Parameters.AddWithValue("@AppointmentDate", appointmentDate);
                    insertLog.Parameters.AddWithValue("@AssessmentId", assessmentId);
                    insertLog.Parameters.AddWithValue("@Uid", (object?)azureUid ?? DBNull.Value);

                    await insertLog.ExecuteNonQueryAsync(ct);
                    logger.LogInformation("Kandidaat toegevoegd aan SentCandidatesToAzure.");
                    WaitForUserIfDebug($"Insert voltooid voor ProjectCandidateId {projectCandidateId}. Controleer nu de database!");
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Fout bij loggen in SentCandidatesToAzure voor ProjectCandidateId {Id}", projectCandidateId);
                }
            }

            logger.LogInformation("Taak 1 klaar. {Count} kandidaten verwerkt.", count);
        }

        public static async Task UpdateCompletionStatusesAsync(ILogger logger, CancellationToken ct)
        {
            logger.LogInformation("Start taak 2: Controleren op voltooide oefeningen...");

            var config = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: false)
                .AddEnvironmentVariables()
                .Build();

            string quintessenceDbConn = config.GetConnectionString("DefaultConnection");
            string gamifiedReadConn = config.GetConnectionString("GamifiedRead");

            try
            {
                using var localConn = new SqlConnection(quintessenceDbConn);
                await localConn.OpenAsync(ct);

                var selectCmd = new SqlCommand(@"
            SELECT ProjectCandidateId, Uid
            FROM dbo.SentCandidatesToAzure
            WHERE Completed = 0 AND Uid IS NOT NULL
        ", localConn);

                using var reader = await selectCmd.ExecuteReaderAsync(ct);

                var candidatesToCheck = new List<(Guid ProjectCandidateId, Guid Uid)>();

                while (await reader.ReadAsync(ct))
                {
                    candidatesToCheck.Add((
                        reader.GetGuid(0),
                        reader.GetGuid(1)
                    ));
                }

                if (candidatesToCheck.Count == 0)
                {
                    logger.LogInformation("Geen openstaande kandidaten om te controleren.");
                    return;
                }

                logger.LogInformation("{Count} kandidaten controleren op voltooiing...", candidatesToCheck.Count);

                foreach (var candidate in candidatesToCheck)
                {
                    ct.ThrowIfCancellationRequested();

                    int? finishedAssignmentId = null;
                    bool isCompleted = false;

                    try
                    {
                        using var gamifiedConn = new SqlConnection(gamifiedReadConn);
                        await gamifiedConn.OpenAsync(ct);

                        var checkCmd = new SqlCommand(@"
                    SELECT finished.Id
                    FROM [Quintessence.GamifiedAssessment].[UserAssessmentManifests] um
                    LEFT JOIN [Quintessence.GamifiedAssessment].[FinishedAssignments] finished
                        ON um.Id = finished.UserAssessmentManifestId
                    WHERE um.Uid = @Uid 
                        AND finished.Id IS NOT NULL
                ", gamifiedConn);

                        checkCmd.Parameters.AddWithValue("@Uid", candidate.Uid);

                        var result = await checkCmd.ExecuteScalarAsync(ct);

                        if (result != null && int.TryParse(result.ToString(), out int parsedId))
                        {
                            finishedAssignmentId = parsedId;
                            isCompleted = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex,
                            "⚠️ Fout bij controleren van kandidaat {ProjectCandidateId}",
                            candidate.ProjectCandidateId);
                        continue;
                    }

                    if (!isCompleted)
                    {
                        logger.LogInformation("Kandidaat {Id} nog niet voltooid.", candidate.ProjectCandidateId);
                        continue;
                    }

                    try
                    {
                        using var updateConn = new SqlConnection(quintessenceDbConn);
                        await updateConn.OpenAsync(ct);

                        var updateCmd = new SqlCommand(@"
                    UPDATE dbo.SentCandidatesToAzure
                    SET Completed = 1,
                        FinishedAssignmentId = @FinishedAssignmentId
                    WHERE ProjectCandidateId = @ProjectCandidateId
                ", updateConn);

                        updateCmd.Parameters.AddWithValue("@ProjectCandidateId", candidate.ProjectCandidateId);
                        updateCmd.Parameters.AddWithValue("@FinishedAssignmentId",
                            (object?)finishedAssignmentId ?? DBNull.Value);

                        await updateCmd.ExecuteNonQueryAsync(ct);

                        logger.LogInformation("Kandidaat {Id} gemarkeerd als voltooid.", candidate.ProjectCandidateId);
                        WaitForUserIfDebug($"Kandidaat {candidate.ProjectCandidateId} is nu Completed=1. Controleer database voor FinishedAssignmentId!");
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex,
                            "Fout bij updaten van kandidaat {ProjectCandidateId}",
                            candidate.ProjectCandidateId);
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Algemene fout in taak 2");
            }
        }

        public static async Task ReplicateResultsAsync(ILogger logger, CancellationToken ct)
        {
            logger.LogInformation("Start taak 3: Resultaten repliceren naar lokale database...");

            var config = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            string quintessenceDbConn = config.GetConnectionString("DefaultConnection");
            string gamifiedReadConn = config.GetConnectionString("GamifiedRead");

            if (string.IsNullOrWhiteSpace(gamifiedReadConn))
                throw new InvalidOperationException("Missing ConnectionStrings:GamifiedRead (env var ConnectionStrings__GamifiedRead).");

            try
            {
                using var localConn = new SqlConnection(quintessenceDbConn);
                await localConn.OpenAsync(ct);

                var selectCmd = new SqlCommand(@"
            SELECT ProjectCandidateId, FinishedAssignmentId
            FROM dbo.SentCandidatesToAzure
            WHERE Completed = 1 AND [DataSyncedOn] IS NULL AND FinishedAssignmentId IS NOT NULL
        ", localConn);

                using var reader = await selectCmd.ExecuteReaderAsync(ct);

                var assignmentsToReplicate = new List<(Guid ProjectCandidateId, int FinishedAssignmentId)>();

                while (await reader.ReadAsync(ct))
                {
                    assignmentsToReplicate.Add((
                        reader.GetGuid(0),
                        reader.GetInt32(1)
                    ));
                }

                if (assignmentsToReplicate.Count == 0)
                {
                    logger.LogInformation("Geen te repliceren kandidaten gevonden.");
                    return;
                }

                foreach (var assignment in assignmentsToReplicate)
                {
                    ct.ThrowIfCancellationRequested();

                    logger.LogInformation("Replicatie starten voor ProjectCandidateId: {Id}", assignment.ProjectCandidateId);

                    try
                    {
                        // 1) FinishedAssignments ophalen + lokaal inserten
                        string? audioId = null;
                        string? screenId = null;
                        string? videoId = null;

                        using (var gamifiedConnFinished = new SqlConnection(gamifiedReadConn))
                        {
                            await gamifiedConnFinished.OpenAsync(ct);

                            var finishedCmd = new SqlCommand(@"
                        SELECT Id, AudioRecordingId, ScreenRecordingId, VideoRecordingId, Feedback
                        FROM [Quintessence.GamifiedAssessment].[FinishedAssignments]
                        WHERE Id = @Id
                    ", gamifiedConnFinished);

                            finishedCmd.Parameters.AddWithValue("@Id", assignment.FinishedAssignmentId);

                            using var finishedReader = await finishedCmd.ExecuteReaderAsync(ct);
                            if (await finishedReader.ReadAsync(ct))
                            {
                                long finishedId = finishedReader.GetInt64(0);
                                audioId = finishedReader.IsDBNull(1) ? null : finishedReader.GetString(1);
                                screenId = finishedReader.IsDBNull(2) ? null : finishedReader.GetString(2);
                                videoId = finishedReader.IsDBNull(3) ? null : finishedReader.GetString(3);
                                var feedback = finishedReader.IsDBNull(4) ? null : finishedReader.GetString(4);

                                using var insertConn = new SqlConnection(quintessenceDbConn);
                                await insertConn.OpenAsync(ct);

                                var insertFinished = new SqlCommand(@"
                            INSERT INTO GamifiedFinishedAssignments 
                            (Id, AudioRecordingId, ScreenRecordingId, VideoRecordingId, Feedback)
                            VALUES (@Id, @Audio, @Screen, @Video, @Feedback)
                        ", insertConn);

                                insertFinished.Parameters.AddWithValue("@Id", finishedId);
                                insertFinished.Parameters.AddWithValue("@Audio", (object?)audioId ?? DBNull.Value);
                                insertFinished.Parameters.AddWithValue("@Screen", (object?)screenId ?? DBNull.Value);
                                insertFinished.Parameters.AddWithValue("@Video", (object?)videoId ?? DBNull.Value);
                                insertFinished.Parameters.AddWithValue("@Feedback", (object?)feedback ?? DBNull.Value);

                                await insertFinished.ExecuteNonQueryAsync(ct);

                                // 2) Downloads + ffmpeg merge + VideoUrl update (zoals origineel)
                                var publicUrl = await DownloadAndMergeRecordingsAsync(
                                    logger: logger,
                                    ct: ct,
                                    config: config,
                                    quintessenceDbConn: quintessenceDbConn,
                                    finishedAssignmentId: assignment.FinishedAssignmentId,
                                    videoId: videoId,
                                    audioId: audioId,
                                    screenId: screenId
                                );

                                if (!string.IsNullOrWhiteSpace(publicUrl))
                                {
                                    using var updateVideoConn = new SqlConnection(quintessenceDbConn);
                                    await updateVideoConn.OpenAsync(ct);

                                    var updateCmd = new SqlCommand(@"
                                UPDATE GamifiedFinishedAssignments
                                SET VideoUrl = @VideoUrl
                                WHERE Id = @Id
                            ", updateVideoConn);

                                    updateCmd.Parameters.AddWithValue("@VideoUrl", publicUrl);
                                    updateCmd.Parameters.AddWithValue("@Id", assignment.FinishedAssignmentId);

                                    await updateCmd.ExecuteNonQueryAsync(ct);

                                    logger.LogInformation("🔗 VideoUrl opgeslagen: {Url}", publicUrl);
                                }
                            }
                        }

                        // 3) Logs repliceren (GamifiedLogs)
                        await ReplicateLogsAsync(logger, ct, quintessenceDbConn, gamifiedReadConn, assignment.FinishedAssignmentId);

                        // 4) Conversations repliceren (GamifiedFinishedAssignmentsConversations)
                        await ReplicateConversationsAsync(logger, ct, quintessenceDbConn, gamifiedReadConn, assignment.FinishedAssignmentId);

                        // 5) DataSyncedOn bijwerken
                        using (var updateConn = new SqlConnection(quintessenceDbConn))
                        {
                            await updateConn.OpenAsync(ct);

                            var updateReport = new SqlCommand(@"
                        UPDATE dbo.SentCandidatesToAzure
                        SET [DataSyncedOn] = GETDATE()
                        WHERE ProjectCandidateId = @ProjectCandidateId
                    ", updateConn);

                            updateReport.Parameters.AddWithValue("@ProjectCandidateId", assignment.ProjectCandidateId);
                            await updateReport.ExecuteNonQueryAsync(ct);
                        }

                        logger.LogInformation("Data gerepliceerd voor kandidaat {Id}.", assignment.ProjectCandidateId);
                        WaitForUserIfDebug($"Replicatie voltooid voor {assignment.ProjectCandidateId}. Controleer GamifiedFinishedAssignments, GamifiedLogs, GamifiedFinishedAssignmentsConversations tabellen!");
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Fout bij replicatie voor kandidaat {Id}", assignment.ProjectCandidateId);
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Algemene fout tijdens replicatie");
            }
        }

        private static async Task ReplicateLogsAsync(
    ILogger logger,
    CancellationToken ct,
    string quintessenceDbConn,
    string gamifiedReadConn,
    int finishedAssignmentId)
        {
            using var gamifiedConnLogs = new SqlConnection(gamifiedReadConn);
            await gamifiedConnLogs.OpenAsync(ct);

            var logsCmd = new SqlCommand(@"
        SELECT @Id, logs.ActionId, logs.[TimeStamp], logs.Details, logs.CreatedOn, logs.ModifiedOn
        FROM [Quintessence.GamifiedAssessment].[Logs] logs
        LEFT JOIN [Quintessence.GamifiedAssessment].[FinishedAssignments] finished
            ON logs.UserAssessmentManifestId = finished.UserAssessmentManifestId
        WHERE finished.Id = @Id
    ", gamifiedConnLogs);

            logsCmd.Parameters.AddWithValue("@Id", finishedAssignmentId);

            using var logsReader = await logsCmd.ExecuteReaderAsync(ct);

            using var logInsertConn = new SqlConnection(quintessenceDbConn);
            await logInsertConn.OpenAsync(ct);

            int inserted = 0;

            while (await logsReader.ReadAsync(ct))
            {
                var insertLog = new SqlCommand(@"
            INSERT INTO GamifiedLogs 
            (Id, ActionId, TimeStamp, Details, CreatedOn, ModifiedOn)
            VALUES (@Id, @ActionId, @TimeStamp, @Details, @CreatedOn, @ModifiedOn)
        ", logInsertConn);

                insertLog.Parameters.AddWithValue("@Id", logsReader.GetInt32(0));
                insertLog.Parameters.AddWithValue("@ActionId", logsReader.GetInt32(1));
                insertLog.Parameters.AddWithValue("@TimeStamp", logsReader.GetFieldValue<DateTimeOffset>(2));
                insertLog.Parameters.AddWithValue("@Details", logsReader.IsDBNull(3) ? DBNull.Value : logsReader.GetString(3));
                insertLog.Parameters.AddWithValue("@CreatedOn", logsReader.GetFieldValue<DateTimeOffset>(4));
                insertLog.Parameters.AddWithValue("@ModifiedOn", logsReader.GetFieldValue<DateTimeOffset>(5));

                await insertLog.ExecuteNonQueryAsync(ct);
                inserted++;
            }

            logger.LogInformation("Logs gerepliceerd voor FinishedAssignmentId {Id}: {Count}", finishedAssignmentId, inserted);
        }

        private static async Task ReplicateConversationsAsync(
            ILogger logger,
            CancellationToken ct,
            string quintessenceDbConn,
            string gamifiedReadConn,
            int finishedAssignmentId)
        {
            using var gamifiedConnConv = new SqlConnection(gamifiedReadConn);
            await gamifiedConnConv.OpenAsync(ct);

            var convCmd = new SqlCommand(@"
        SELECT FinishedAssignmentId, ConversationId, SelectedChoice, Answer, CreatedOn, ModifiedOn
        FROM [Quintessence.GamifiedAssessment].[FinishedAssignmentConversations]
        WHERE FinishedAssignmentId = @Id
    ", gamifiedConnConv);

            convCmd.Parameters.AddWithValue("@Id", finishedAssignmentId);

            using var convReader = await convCmd.ExecuteReaderAsync(ct);

            using var convInsertConn = new SqlConnection(quintessenceDbConn);
            await convInsertConn.OpenAsync(ct);

            int inserted = 0;

            while (await convReader.ReadAsync(ct))
            {
                var insertConv = new SqlCommand(@"
            INSERT INTO GamifiedFinishedAssignmentsConversations
            (Id, ConversationId, SelectedChoice, Answer, CreatedOn, ModifiedOn)
            VALUES (@Id, @ConversationId, @SelectedChoice, @Answer, @CreatedOn, @ModifiedOn)
        ", convInsertConn);

                insertConv.Parameters.AddWithValue("@Id", convReader.GetInt64(0));
                insertConv.Parameters.AddWithValue("@ConversationId", convReader.GetGuid(1));
                insertConv.Parameters.AddWithValue("@SelectedChoice", convReader.IsDBNull(2) ? DBNull.Value : convReader.GetString(2));
                insertConv.Parameters.AddWithValue("@Answer", convReader.IsDBNull(3) ? DBNull.Value : convReader.GetString(3));
                insertConv.Parameters.AddWithValue("@CreatedOn", convReader.GetFieldValue<DateTimeOffset>(4));
                insertConv.Parameters.AddWithValue("@ModifiedOn", convReader.GetFieldValue<DateTimeOffset>(5));

                await insertConv.ExecuteNonQueryAsync(ct);
                inserted++;
            }

            logger.LogInformation("Conversations gerepliceerd voor FinishedAssignmentId {Id}: {Count}", finishedAssignmentId, inserted);
        }

        private static async Task<string?> DownloadAndMergeRecordingsAsync(
            ILogger logger,
            CancellationToken ct,
            IConfiguration config,
            string quintessenceDbConn,
            long finishedAssignmentId,
            string? videoId,
            string? audioId,
            string? screenId)
        {
            try
            {
                string? sasToken = config["Gamified:SasToken"];
                string? baseUrl = config["Gamified:RecordingBaseUrl"];

                if (string.IsNullOrWhiteSpace(sasToken))
                    throw new InvalidOperationException("Missing Gamified:SasToken (env var Gamified__SasToken).");

                if (string.IsNullOrWhiteSpace(baseUrl))
                    throw new InvalidOperationException("Missing Gamified:RecordingBaseUrl.");

                string tempFolder = Path.Combine(AppContext.BaseDirectory, "temp_fragments", finishedAssignmentId.ToString());
                Directory.CreateDirectory(tempFolder);

                logger.LogInformation("⬇️ Start downloaden voor FinishedAssignmentId {Id}...", finishedAssignmentId);

                var fragmentIds = new[] { videoId, audioId, screenId };
                var localNames = new[] { "video1.mp4", "video2.mp4", "video3.mp4" };
                var downloadedFiles = new string[3];

                using (var httpClient = new HttpClient())
                {
                    for (int i = 0; i < fragmentIds.Length; i++)
                    {
                        ct.ThrowIfCancellationRequested();

                        if (string.IsNullOrWhiteSpace(fragmentIds[i]))
                        {
                            downloadedFiles[i] = string.Empty;
                            continue;
                        }

                        string downloadUrl = $"{baseUrl}{fragmentIds[i]}?{sasToken}";
                        string outputPath = Path.Combine(tempFolder, localNames[i]);

                        using var response = await httpClient.GetAsync(downloadUrl, ct);
                        response.EnsureSuccessStatusCode();

                        await using var fs = new FileStream(outputPath, FileMode.Create, FileAccess.Write, FileShare.None);
                        await response.Content.CopyToAsync(fs);

                        downloadedFiles[i] = outputPath;
                        logger.LogInformation("Gedownload: {Path}", outputPath);
                    }
                }

                string video1 = downloadedFiles[0];
                string video2 = downloadedFiles[1];
                string video3 = downloadedFiles[2];

                if (!File.Exists(video1) || !File.Exists(video2) || !File.Exists(video3))
                {
                    logger.LogWarning("Niet alle bestanden beschikbaar om samen te voegen voor {Id}.", finishedAssignmentId);
                    return null;
                }

                string fileName = $"Gamified_{finishedAssignmentId}.mp4";

                // Outputfolder bepalen via candidateId lookup (zoals origineel)
                string oDirectory;
                string publicUrl;

                using (var lookupConn = new SqlConnection(quintessenceDbConn))
                {
                    await lookupConn.OpenAsync(ct);

                    var cmd = new SqlCommand(@"
                SELECT c.Id
                FROM Quintessence.dbo.GamifiedFinishedAssignments fa
                LEFT JOIN Quintessence.dbo.SentCandidatesToAzure azure ON fa.Id = azure.FinishedAssignmentId
                LEFT JOIN Quintessence.dbo.ProjectCandidateView pv ON azure.ProjectCandidateId = pv.Id
                LEFT JOIN DataWarehouse.dbo.QuintessencePortalSubmissionCandidate c ON pv.CrmCandidateInfoId = c.QplanetCandidateInfoId
                WHERE fa.Id = @Id
            ", lookupConn);

                    cmd.Parameters.AddWithValue("@Id", finishedAssignmentId);

                    var result = await cmd.ExecuteScalarAsync(ct);

                    var uploadsRoot = config["Paths:PortalUploadsRootUNC"] ?? throw new InvalidOperationException("Missing Paths:PortalUploadsRootUNC");
                    var uploadsPublic = config["Paths:PortalUploadsPublicBaseUrl"] ?? throw new InvalidOperationException("Missing Paths:PortalUploadsPublicBaseUrl");
                    var gamifiedRoot = config["Paths:PortalGamifiedRootUNC"] ?? throw new InvalidOperationException("Missing Paths:PortalGamifiedRootUNC");
                    var gamifiedPublic = config["Paths:PortalGamifiedPublicBaseUrl"] ?? throw new InvalidOperationException("Missing Paths:PortalGamifiedPublicBaseUrl");

                    uploadsRoot = NormalizeUnc(uploadsRoot);
                    gamifiedRoot = NormalizeUnc(gamifiedRoot);

                    if (result != null && Guid.TryParse(result.ToString(), out Guid candidateId))
                    {
                        oDirectory = Path.Combine(uploadsRoot, candidateId.ToString());
                        publicUrl = $"{uploadsPublic}{candidateId}/{fileName}";
                    }
                    else
                    {
                        oDirectory = gamifiedRoot;
                        publicUrl = $"{gamifiedPublic}{fileName}";
                    }
                }

                logger.LogWarning("Output dir (raw): {Dir}", oDirectory);

                Directory.CreateDirectory(oDirectory);
                string outputMp4 = Path.Combine(oDirectory, fileName);

                string ffmpegExe = config["Paths:FfmpegExe"] ?? throw new InvalidOperationException("Missing Paths:FfmpegExe");

                string ffmpegArgs =
                    $"-y -i \"{video1}\" -i \"{video2}\" -i \"{video3}\" " +
                    "-filter_complex \"[0:v]scale=-2:480[left];[2:v]scale=-2:480[right];[left][right]hstack=inputs=2[out]\" " +
                    "-map \"[out]\" -map 1:a -c:v libx264 -c:a aac -strict experimental -shortest " +
                    $"\"{outputMp4}\"";

                logger.LogInformation("Start samenvoegen met ffmpeg: {Output}", outputMp4);

                var p = new Process();
                p.StartInfo.FileName = ffmpegExe;
                p.StartInfo.Arguments = ffmpegArgs;
                p.StartInfo.RedirectStandardError = true;
                p.StartInfo.RedirectStandardOutput = true;
                p.StartInfo.UseShellExecute = false;
                p.StartInfo.CreateNoWindow = true;

                p.Start();

                // ffmpeg kan lang duren; we checken cancellation vóór/na
                ct.ThrowIfCancellationRequested();

                string stderr = await p.StandardError.ReadToEndAsync();
                string stdout = await p.StandardOutput.ReadToEndAsync();
                p.WaitForExit();

                if (p.ExitCode != 0)
                {
                    logger.LogError("ffmpeg failed (ExitCode {Code}). STDERR: {Err}", p.ExitCode, stderr);
                    return null;
                }

                logger.LogInformation("Samengevoegd bestand aangemaakt: {Path}", outputMp4);

                // (optioneel) opruimen temp folder — voorlopig laten staan zoals origineel
                // Directory.Delete(tempFolder, recursive: true);

                return publicUrl;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Fout tijdens downloaden/samenvoegen voor ID {Id}", finishedAssignmentId);
                return null;
            }
        }

        static string NormalizeUnc(string p)
        {
            if (string.IsNullOrWhiteSpace(p)) return p;

            // Fix accidental triple backslash at start: \\\server -> \\server
            if (p.StartsWith(@"\\\"))
                p = @"\\" + p.Substring(3);

            return p;
        }

        static async Task<string?> GetAccessTokenAsync()
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            var tenantId = config["AzureAd:TenantId"];
            var clientId = config["AzureAd:ClientId"];
            var clientSecret = config["AzureAd:ClientSecret"]; // env var AzureAd__ClientSecret
            var scope = config["AzureAd:Scope"];

            if (string.IsNullOrWhiteSpace(clientSecret))
                throw new InvalidOperationException(
                    "Missing AzureAd:ClientSecret (env var AzureAd__ClientSecret)."
                );

            var tokenEndpoint =
                $"https://login.microsoftonline.com/{tenantId}/oauth2/v2.0/token";

            using var client = new HttpClient();

            var body = new FormUrlEncodedContent(new[]
            {
        new KeyValuePair<string, string>("grant_type", "client_credentials"),
        new KeyValuePair<string, string>("client_id", clientId),
        new KeyValuePair<string, string>("client_secret", clientSecret),
        new KeyValuePair<string, string>("scope", scope),
    });

            var response = await client.PostAsync(tokenEndpoint, body);

            if (!response.IsSuccessStatusCode)
            {
                var error = await response.Content.ReadAsStringAsync();
                Console.WriteLine($"Token ophalen mislukt: {response.StatusCode} - {error}");
                return null;
            }

            var responseContent = await response.Content.ReadAsStringAsync();
            var jsonDoc = JsonDocument.Parse(responseContent);

            return jsonDoc.RootElement
                .GetProperty("access_token")
                .GetString();
        }
    }
}
