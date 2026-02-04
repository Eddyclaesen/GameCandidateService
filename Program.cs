using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

namespace GameCandidateService
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string quintessenceDbConn = "Server=10.3.176.13\\QNT02S;Database=Quintessence;User ID=QuintessenceUser;Password=$Quint123;Encrypt=True;TrustServerCertificate=True;";

            Console.WriteLine("=== GameCandidateService gestart ===");

            while (true)
            {
                Console.WriteLine($"[{DateTime.Now}] Nieuwe iteratie gestart...");

                try
                {
                    using var connection = new SqlConnection(quintessenceDbConn);
                    using var command = new SqlCommand("dbo.game_GetCandidates", connection)
                    {
                        CommandType = CommandType.StoredProcedure
                    };

                    await connection.OpenAsync();
                    using var reader = await command.ExecuteReaderAsync();

                    int count = 0;
                    while (await reader.ReadAsync())
                    {
                        var projectCandidateId = reader.GetGuid(0);
                        var candidateUserId = reader.GetGuid(1);
                        var language = reader.GetString(2);
                        var manifestId = reader.GetInt32(3);
                        var appointmentDate = reader.GetDateTime(4);
                        var assessmentId = reader.GetString(5);

                        Console.WriteLine($"Kandidaat {++count}:");
                        Console.WriteLine($"  - CandidateUserId: {candidateUserId}");
                        Console.WriteLine($"  - ManifestId: {manifestId}");
                        Console.WriteLine($"  - Language: {language}");
                        Console.WriteLine($"  - AppointmentDate: {appointmentDate}");
                        Console.WriteLine($"  - AssessmentId: {assessmentId}");

                        // Stap 1: Token ophalen
                        var token = await GetAccessTokenAsync();
                        if (string.IsNullOrEmpty(token))
                        {
                            Console.WriteLine("❌ Token ophalen mislukt. Sla kandidaat over.");
                            continue;
                        }

                        // JSON body samenstellen
                        var jsonBody = new
                        {
                            assessmentId = assessmentId,
                            language = language,
                            startTime = DateTime.UtcNow.Date.ToString("yyyy-MM-dd"),
                            endTime = appointmentDate.AddDays(100).ToString("yyyy-MM-dd")
                        };

                        var apiUrl = $"https://gamifiedmanagement.quintessence.be/api/assessments/{candidateUserId}";

                        try
                        {
                            using var httpClient = new HttpClient();
                            httpClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);

                            var content = new StringContent(JsonSerializer.Serialize(jsonBody));
                            content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");

                            var response = await httpClient.PostAsync(apiUrl, content);

                            if (response.IsSuccessStatusCode)
                            {
                                Console.WriteLine("✅ POST naar Gamified API succesvol.");

                                Guid? azureUid = null;

                                // Nieuwe connectionstring voor read-only toegang tot Gamified DB
                                string gamifiedReadConn = "Server=tcp:qgamified-sql-prd-weu.database.windows.net,1433;" +
                                                          "Initial Catalog=sqldb-qgamified-weu;" +
                                                          "Persist Security Info=False;" +
                                                          "User ID=ReadOnlyUser;" +
                                                          "Password=Nd8BNJVBtZ*y4cHG@qcsbNBx;" +
                                                          "MultipleActiveResultSets=False;" +
                                                          "Encrypt=True;" +
                                                          "TrustServerCertificate=True;" +
                                                          "Connection Timeout=30;";

                                try
                                {
                                    using var uidConn = new SqlConnection(gamifiedReadConn);
                                    await uidConn.OpenAsync();

                                    var uidCmd = new SqlCommand(@"
                                        SELECT TOP 1 Uid
                                        FROM [Quintessence.GamifiedAssessment].[UserAssessmentManifests]
                                        WHERE UserId = @UserId
                                        ORDER BY Id DESC
                                    ", uidConn);

                                    uidCmd.Parameters.AddWithValue("@UserId", candidateUserId);

                                    var result = await uidCmd.ExecuteScalarAsync();

                                    if (result != null && Guid.TryParse(result.ToString(), out Guid parsedUid))
                                    {
                                        azureUid = parsedUid;
                                        Console.WriteLine($"🔗 Azure Uid opgehaald: {azureUid}");
                                    }
                                    else
                                    {
                                        Console.WriteLine("⚠️ Geen Uid gevonden in Azure via ReadOnly-verbinding.");
                                    }
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"❌ Fout bij ophalen van Azure Uid via ReadOnly verbinding: {ex.Message}");
                                }

                                // Opslaan in lokale log-tabel om duplicaten te vermijden
                                try
                                {
                                    using var insertConn = new SqlConnection(quintessenceDbConn);
                                    await insertConn.OpenAsync();

                                    var insertLog = new SqlCommand(@"
                                        INSERT INTO dbo.SentCandidatesToAzure 
                                        (ProjectCandidateId, CandidateUserId, Language, ManifestId, AppointmentDate, AssessmentId, SendToAzure, Completed, Uid)
                                        VALUES (@ProjectCandidateId, @CandidateUserId, @Language, @ManifestId, @AppointmentDate, @AssessmentId, GETDATE(), 0, @Uid);
                                    ", insertConn);

                                    insertLog.Parameters.AddWithValue("@ProjectCandidateId", projectCandidateId);
                                    insertLog.Parameters.AddWithValue("@CandidateUserId", candidateUserId);
                                    insertLog.Parameters.AddWithValue("@Language", language);
                                    insertLog.Parameters.AddWithValue("@ManifestId", manifestId);
                                    insertLog.Parameters.AddWithValue("@AppointmentDate", appointmentDate);
                                    insertLog.Parameters.AddWithValue("@AssessmentId", assessmentId);
                                    insertLog.Parameters.AddWithValue("@Uid", (object?)azureUid ?? DBNull.Value);

                                    await insertLog.ExecuteNonQueryAsync();
                                    Console.WriteLine("📥 Kandidaat toegevoegd aan SentCandidatesToAzure.");
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"⚠️ Fout bij loggen in SentCandidatesToAzure: {ex.Message}");
                                }

                            }
                            else
                            {
                                Console.WriteLine($"❌ Fout bij POST: {response.StatusCode}");
                                var errorContent = await response.Content.ReadAsStringAsync();
                                Console.WriteLine(errorContent);
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Fout tijdens POST-aanvraag: {ex.Message}");
                        }
                    }

                    Console.WriteLine(count == 0 ? "Geen kandidaten gevonden." : $"{count} kandidaten verwerkt.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine("❌ Fout in hoofdprocedure:");
                    Console.WriteLine(ex.Message);
                }

                Console.WriteLine("✔ Eerste taak (verzenden naar Azure) voltooid.");
                Console.WriteLine("▶️ Start tweede taak: controleren op voltooide oefeningen...");
                await UpdateCompletionStatusesAsync();
                await ReplicateResultsAsync();

                Console.WriteLine("⏳ Wachten tot volgende run (5 minuten)...\n");
                await Task.Delay(TimeSpan.FromMinutes(5));
            }
        }

        static async Task UpdateCompletionStatusesAsync()
        {
            Console.WriteLine("▶️ Start taak 2: Controleren op voltooide oefeningen...");

            string quintessenceDbConn = "Server=10.3.176.13\\QNT02S;Database=Quintessence;User ID=QuintessenceUser;Password=$Quint123;Encrypt=True;TrustServerCertificate=True;";
            string gamifiedReadConn = "Server=tcp:qgamified-sql-prd-weu.database.windows.net,1433;" +
                                      "Initial Catalog=sqldb-qgamified-weu;" +
                                      "Persist Security Info=False;" +
                                      "User ID=ReadOnlyUser;" +
                                      "Password=Nd8BNJVBtZ*y4cHG@qcsbNBx;" +
                                      "MultipleActiveResultSets=False;" +
                                      "Encrypt=True;" +
                                      "TrustServerCertificate=True;" +
                                      "Connection Timeout=30;";

            try
            {
                using var localConn = new SqlConnection(quintessenceDbConn);
                await localConn.OpenAsync();

                var selectCmd = new SqlCommand(@"
                    SELECT ProjectCandidateId, Uid
                    FROM dbo.SentCandidatesToAzure
                    WHERE Completed = 0 AND Uid IS NOT NULL
                ", localConn);

                using var reader = await selectCmd.ExecuteReaderAsync();
                var candidatesToCheck = new List<(Guid ProjectCandidateId, Guid Uid)>();

                while (await reader.ReadAsync())
                {
                    candidatesToCheck.Add((
                        reader.GetGuid(0), // ProjectCandidateId
                        reader.GetGuid(1)  // Uid
                    ));
                }

                if (candidatesToCheck.Count == 0)
                {
                    Console.WriteLine("📭 Geen openstaande kandidaten om te controleren.");
                    return;
                }

                Console.WriteLine($"🔍 {candidatesToCheck.Count} kandidaten controleren op voltooiing...");

                foreach (var candidate in candidatesToCheck)
                {
                    int? finishedAssignmentId = null;
                    bool isCompleted = false;

                    try
                    {
                        using var gamifiedConn = new SqlConnection(gamifiedReadConn);
                        await gamifiedConn.OpenAsync();

                        var checkCmd = new SqlCommand(@"
                            SELECT finished.Id
	                        FROM [Quintessence.GamifiedAssessment].[UserAssessmentManifests] um
	                        LEFT JOIN [Quintessence.GamifiedAssessment].[FinishedAssignments] finished
		                        ON um.Id = finished.UserAssessmentManifestId
	                        WHERE um.Uid = @Uid 
		                        AND finished.Id IS NOT NULL
                        ", gamifiedConn);

                        checkCmd.Parameters.AddWithValue("@Uid", candidate.Uid);
                        var result = await checkCmd.ExecuteScalarAsync();

                        if (result != null && int.TryParse(result.ToString(), out int parsedId))
                        {
                            finishedAssignmentId = parsedId;
                            isCompleted = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"⚠️ Fout bij controleren van kandidaat {candidate.ProjectCandidateId}: {ex.Message}");
                        continue;
                    }

                    if (isCompleted)
                    {
                        try
                        {
                            using var updateConn = new SqlConnection(quintessenceDbConn);
                            await updateConn.OpenAsync();

                            var updateCmd = new SqlCommand(@"
                                UPDATE dbo.SentCandidatesToAzure
                                SET Completed = 1,
                                    FinishedAssignmentId = @FinishedAssignmentId
                                WHERE ProjectCandidateId = @ProjectCandidateId
                            ", updateConn);

                            updateCmd.Parameters.AddWithValue("@ProjectCandidateId", candidate.ProjectCandidateId);
                            updateCmd.Parameters.AddWithValue("@FinishedAssignmentId", (object?)finishedAssignmentId ?? DBNull.Value);

                            await updateCmd.ExecuteNonQueryAsync();

                            Console.WriteLine($"✅ Kandidaat {candidate.ProjectCandidateId} gemarkeerd als voltooid.");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"❌ Fout bij updaten van kandidaat {candidate.ProjectCandidateId}: {ex.Message}");
                        }
                    }
                    else
                    {
                        Console.WriteLine($"🕒 Kandidaat {candidate.ProjectCandidateId} heeft oefening nog niet voltooid.");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Algemene fout in taak 2: {ex.Message}");
            }
        }

        static async Task ReplicateResultsAsync()
        {
            Console.WriteLine("▶️ Start taak 3: Resultaten repliceren naar lokale database...");

            string quintessenceDbConn = "Server=10.3.176.13\\QNT02S;Database=Quintessence;User ID=QuintessenceUser;Password=$Quint123;Encrypt=True;TrustServerCertificate=True;";
            string gamifiedReadConn = "Server=tcp:qgamified-sql-prd-weu.database.windows.net,1433;" +
                                      "Initial Catalog=sqldb-qgamified-weu;" +
                                      "Persist Security Info=False;" +
                                      "User ID=ReadOnlyUser;" +
                                      "Password=Nd8BNJVBtZ*y4cHG@qcsbNBx;" +
                                      "MultipleActiveResultSets=False;" +
                                      "Encrypt=True;" +
                                      "TrustServerCertificate=True;" +
                                      "Connection Timeout=30;";

            try
            {
                using var localConn = new SqlConnection(quintessenceDbConn);
                await localConn.OpenAsync();

                var selectCmd = new SqlCommand(@"
            SELECT ProjectCandidateId, FinishedAssignmentId
            FROM dbo.SentCandidatesToAzure
            WHERE Completed = 1 AND [DataSyncedOn] IS NULL AND FinishedAssignmentId IS NOT NULL
        ", localConn);

                using var reader = await selectCmd.ExecuteReaderAsync();
                var assignmentsToReplicate = new List<(Guid ProjectCandidateId, int FinishedAssignmentId)>();

                while (await reader.ReadAsync())
                {
                    assignmentsToReplicate.Add((
                        reader.GetGuid(0),
                        reader.GetInt32(1)
                    ));
                }

                if (assignmentsToReplicate.Count == 0)
                {
                    Console.WriteLine("📭 Geen te repliceren kandidaten gevonden.");
                    return;
                }

                foreach (var assignment in assignmentsToReplicate)
                {
                    Console.WriteLine($"📤 Replicatie starten voor ProjectCandidateId: {assignment.ProjectCandidateId}");

                    try
                    {
                        // === 1. FinishedAssignments ===
                        using (var gamifiedConnFinished = new SqlConnection(gamifiedReadConn))
                        {
                            await gamifiedConnFinished.OpenAsync();

                            var finishedCmd = new SqlCommand(@"
                        SELECT Id, AudioRecordingId, ScreenRecordingId, VideoRecordingId, Feedback
                        FROM [Quintessence.GamifiedAssessment].[FinishedAssignments]
                        WHERE Id = @Id
                    ", gamifiedConnFinished);
                            finishedCmd.Parameters.AddWithValue("@Id", assignment.FinishedAssignmentId);

                            using var finishedReader = await finishedCmd.ExecuteReaderAsync();
                            if (await finishedReader.ReadAsync())
                            {
                                using var insertConn = new SqlConnection(quintessenceDbConn);
                                await insertConn.OpenAsync();

                                var insertFinished = new SqlCommand(@"
                                    INSERT INTO GamifiedFinishedAssignments 
                                    (Id, AudioRecordingId, ScreenRecordingId, VideoRecordingId, Feedback)
                                    VALUES (@Id, @Audio, @Screen, @Video, @Feedback)
                                ", insertConn);

                                insertFinished.Parameters.AddWithValue("@Id", finishedReader.GetInt64(0));
                                insertFinished.Parameters.AddWithValue("@Audio", finishedReader.IsDBNull(1) ? DBNull.Value : finishedReader.GetString(1));
                                insertFinished.Parameters.AddWithValue("@Screen", finishedReader.IsDBNull(2) ? DBNull.Value : finishedReader.GetString(2));
                                insertFinished.Parameters.AddWithValue("@Video", finishedReader.IsDBNull(3) ? DBNull.Value : finishedReader.GetString(3));
                                insertFinished.Parameters.AddWithValue("@Feedback", finishedReader.IsDBNull(4) ? DBNull.Value : finishedReader.GetString(4));

                                string? audioId = finishedReader.IsDBNull(1) ? null : finishedReader.GetString(1);
                                string? screenId = finishedReader.IsDBNull(2) ? null : finishedReader.GetString(2);
                                string? videoId = finishedReader.IsDBNull(3) ? null : finishedReader.GetString(3);

                                await insertFinished.ExecuteNonQueryAsync();

                                await DownloadAndMergeRecordingsAsync(
                                    finishedAssignmentId: assignment.FinishedAssignmentId,
                                    videoId: videoId,
                                    audioId: audioId,
                                    screenId: screenId);
                            }
                        }

                        // === 2. Logs ===
                        using (var gamifiedConnLogs = new SqlConnection(gamifiedReadConn))
                        {
                            await gamifiedConnLogs.OpenAsync();

                            var logsCmd = new SqlCommand(@"
                        SELECT @Id, logs.ActionId, logs.[TimeStamp], logs.Details, logs.CreatedOn, logs.ModifiedOn
                        FROM [Quintessence.GamifiedAssessment].[Logs] logs
                        LEFT JOIN [Quintessence.GamifiedAssessment].[FinishedAssignments] finished
                            ON logs.UserAssessmentManifestId = finished.UserAssessmentManifestId
                        WHERE finished.Id = @Id
                    ", gamifiedConnLogs);
                            logsCmd.Parameters.AddWithValue("@Id", assignment.FinishedAssignmentId);

                            using var logsReader = await logsCmd.ExecuteReaderAsync();
                            using var logInsertConn = new SqlConnection(quintessenceDbConn);
                            await logInsertConn.OpenAsync();

                            while (await logsReader.ReadAsync())
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

                                await insertLog.ExecuteNonQueryAsync();
                            }
                        }

                        // === 3. Conversations ===
                        using (var gamifiedConnConv = new SqlConnection(gamifiedReadConn))
                        {
                            await gamifiedConnConv.OpenAsync();

                            var convCmd = new SqlCommand(@"
                        SELECT FinishedAssignmentId, ConversationId, SelectedChoice, Answer, CreatedOn, ModifiedOn
                        FROM [Quintessence.GamifiedAssessment].[FinishedAssignmentConversations]
                        WHERE FinishedAssignmentId = @Id
                    ", gamifiedConnConv);
                            convCmd.Parameters.AddWithValue("@Id", assignment.FinishedAssignmentId);

                            using var convReader = await convCmd.ExecuteReaderAsync();
                            using var convInsertConn = new SqlConnection(quintessenceDbConn);
                            await convInsertConn.OpenAsync();

                            while (await convReader.ReadAsync())
                            {
                                var insertConv = new SqlCommand(@"
                            INSERT INTO GamifiedFinishedAssignmentsConversations
                            (Id, ConversationId, SelectedChoice, Answer, CreatedOn, ModifiedOn)
                            VALUES (@Id, @ConversationId, @SelectedChoice, @Answer, @CreatedOn, @ModifiedOn)
                        ", convInsertConn);

                                insertConv.Parameters.AddWithValue("@Id", convReader.GetInt64(0)); // FinishedAssignmentId
                                insertConv.Parameters.AddWithValue("@ConversationId", convReader.GetGuid(1)); // uniqueidentifier
                                insertConv.Parameters.AddWithValue("@SelectedChoice", convReader.IsDBNull(2) ? DBNull.Value : convReader.GetString(2));
                                insertConv.Parameters.AddWithValue("@Answer", convReader.IsDBNull(3) ? DBNull.Value : convReader.GetString(3));
                                insertConv.Parameters.AddWithValue("@CreatedOn", convReader.GetFieldValue<DateTimeOffset>(4));
                                insertConv.Parameters.AddWithValue("@ModifiedOn", convReader.GetFieldValue<DateTimeOffset>(5));

                                await insertConv.ExecuteNonQueryAsync();
                            }
                        }

                        // === 4. DataSyncedOn timestamp bijwerken ===
                        using var updateConn = new SqlConnection(quintessenceDbConn);
                        await updateConn.OpenAsync();

                        var updateReport = new SqlCommand(@"
                            UPDATE dbo.SentCandidatesToAzure
                            SET [DataSyncedOn] = GETDATE()
                            WHERE ProjectCandidateId = @ProjectCandidateId
                        ", updateConn);
                        updateReport.Parameters.AddWithValue("@ProjectCandidateId", assignment.ProjectCandidateId);
                        await updateReport.ExecuteNonQueryAsync();

                        Console.WriteLine($"✅ Data gerepliceerd voor kandidaat {assignment.ProjectCandidateId}.");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"❌ Fout bij replicatie voor kandidaat {assignment.ProjectCandidateId}: {ex.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Algemene fout tijdens replicatie: {ex.Message}");
            }
        }

        // Deze methode kun je integreren in je bestaande replicatieproces na het opslaan van de IDs
        static async Task DownloadAndMergeRecordingsAsync(
            long finishedAssignmentId,
            string? videoId,
            string? audioId,
            string? screenId)
        {
            try
            {
                string sasToken = "sp=r&st=2025-05-13T08:37:26Z&se=2026-05-13T16:37:26Z&spr=https&sv=2024-11-04&sr=c&sig=%2Fxi0UyLaPGKST2q0JTZhUHHyHl7GiqvwU2W6%2Bl%2FIutA%3D";
                string baseUrl = "https://qgamifiedstprdweu01.blob.core.windows.net/assignment-recordings/";
                string tempFolder = Path.Combine(Directory.GetCurrentDirectory(), "temp_fragments", finishedAssignmentId.ToString());
                Directory.CreateDirectory(tempFolder);

                Console.WriteLine($"Start downloaden voor ID {finishedAssignmentId}...");

                var fragmentIds = new[] { videoId, audioId, screenId };
                var localNames = new[] { "video1.mp4", "video2.mp4", "video3.mp4" };
                var downloadedFiles = new string[3];

                using (var httpClient = new HttpClient())
                {
                    for (int i = 0; i < fragmentIds.Length; i++)
                    {
                        if (string.IsNullOrWhiteSpace(fragmentIds[i]))
                        {
                            downloadedFiles[i] = string.Empty;
                            continue;
                        }

                        string downloadUrl = $"{baseUrl}{fragmentIds[i]}?{sasToken}";
                        string outputPath = Path.Combine(tempFolder, localNames[i]);

                        using var response = await httpClient.GetAsync(downloadUrl);
                        response.EnsureSuccessStatusCode();

                        using var fs = new FileStream(outputPath, FileMode.Create, FileAccess.Write);
                        await response.Content.CopyToAsync(fs);

                        downloadedFiles[i] = outputPath;
                        Console.WriteLine($"Gedownload: {outputPath}");
                    }
                }

                // Samenvoegen via ffmpeg
                string video1 = downloadedFiles[0];
                string video2 = downloadedFiles[1];
                string video3 = downloadedFiles[2];
                string fileName = $"Gamified_{finishedAssignmentId}.mp4";

                if (!File.Exists(video1) || !File.Exists(video2) || !File.Exists(video3))
                {
                    Console.WriteLine("❌ Niet alle bestanden beschikbaar om samen te voegen.");
                    return;
                }

                // 🔍 Stap 1: Bepaal dynamisch de outputfolder op basis van c.Id
                string oDirectory;
                string publicUrl;

                using (var lookupConn = new SqlConnection("Server=10.3.176.13\\QNT02S;Database=Quintessence;User ID=QuintessenceUser;Password=$Quint123;Encrypt=True;TrustServerCertificate=True;"))
                {
                    await lookupConn.OpenAsync();
                    var cmd = new SqlCommand(@"
                        SELECT c.Id
                        FROM Quintessence.dbo.GamifiedFinishedAssignments fa
                        LEFT JOIN Quintessence.dbo.SentCandidatesToAzure azure ON fa.Id = azure.FinishedAssignmentId
                        LEFT JOIN Quintessence.dbo.ProjectCandidateView pv ON azure.ProjectCandidateId = pv.Id
                        LEFT JOIN DataWarehouse.dbo.QuintessencePortalSubmissionCandidate c ON pv.CrmCandidateInfoId = c.QplanetCandidateInfoId
                        WHERE fa.Id = @Id
                    ", lookupConn);
                    cmd.Parameters.AddWithValue("@Id", finishedAssignmentId);

                    var result = await cmd.ExecuteScalarAsync();
                    // result = Guid.Parse("58bf0241-59d3-4e2c-b3bd-27f745483596"); // vervang door je gewenste test-ID
                    if (result != null && Guid.TryParse(result.ToString(), out Guid candidateId))
                    {
                        oDirectory = @"\\10.3.176.34\c$\inetpub\wwwroot\Sites\QuintessencePortal\Uploads\Participants\" + candidateId;
                        publicUrl = $"https://qportal.quintessence.be/Uploads/Participants/{candidateId}/{fileName}";
                    }
                    else
                    {
                        oDirectory = @"\\10.3.176.34\c$\inetpub\wwwroot\Sites\QuintessencePortal\Gamified";
                        publicUrl = $"https://qportal.quintessence.be/Gamified/{fileName}";
                    }
                }

                Directory.CreateDirectory(oDirectory);
                string outputMp4 = Path.Combine(oDirectory, fileName);

                string ffmpegExe = @"C:\\ffmpeg\\bin\\ffmpeg.exe";
                string ffmpegArgs =
                    $"-y -i \"{video1}\" -i \"{video2}\" -i \"{video3}\" " +
                    "-filter_complex \"[0:v]scale=-2:480[left];[2:v]scale=-2:480[right];[left][right]hstack=inputs=2[out]\" " +
                    "-map \"[out]\" -map 1:a -c:v libx264 -c:a aac -strict experimental -shortest " +
                    $"\"{outputMp4}\"";

                var ffmpegProcess = new Process();
                ffmpegProcess.StartInfo.FileName = ffmpegExe;
                ffmpegProcess.StartInfo.Arguments = ffmpegArgs;
                ffmpegProcess.StartInfo.RedirectStandardOutput = true;
                ffmpegProcess.StartInfo.RedirectStandardError = true;
                ffmpegProcess.StartInfo.UseShellExecute = false;
                ffmpegProcess.StartInfo.CreateNoWindow = true;

                Console.WriteLine("Start samenvoegen met ffmpeg...");
                ffmpegProcess.Start();
                string output = ffmpegProcess.StandardError.ReadToEnd();
                ffmpegProcess.WaitForExit();

                Console.WriteLine(output);
                Console.WriteLine($"✅ Samengevoegd bestand aangemaakt: {outputMp4}");

                // ✅ Wegschrijven van video pad naar de database
                using var updateConn = new SqlConnection("Server=10.3.176.13\\QNT02S;Database=Quintessence;User ID=QuintessenceUser;Password=$Quint123;Encrypt=True;TrustServerCertificate=True;");
                await updateConn.OpenAsync();

                var updateCmd = new SqlCommand(@"
                    UPDATE GamifiedFinishedAssignments
                    SET VideoUrl = @VideoUrl
                    WHERE Id = @Id
                ", updateConn);

                updateCmd.Parameters.AddWithValue("@VideoUrl", publicUrl);
                updateCmd.Parameters.AddWithValue("@Id", finishedAssignmentId);
                await updateCmd.ExecuteNonQueryAsync();

                // Optioneel: opruimen
                // foreach (var file in downloadedFiles) if (File.Exists(file)) File.Delete(file);
                // Directory.Delete(tempFolder);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Fout tijdens downloaden/samenvoegen voor ID {finishedAssignmentId}: {ex.Message}");
            }
        }

        static async Task<string> GetAccessTokenAsync()
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            var tenantId = config["AzureAd:TenantId"];
            var clientId = config["AzureAd:ClientId"];
            var clientSecret = config["AzureAd:ClientSecret"]; // komt uit env var AzureAd__ClientSecret
            var scope = config["AzureAd:Scope"];

            if (string.IsNullOrWhiteSpace(clientSecret))
                throw new InvalidOperationException("Missing AzureAd:ClientSecret (env var AzureAd__ClientSecret).");
            var tokenEndpoint = $"https://login.microsoftonline.com/{tenantId}/oauth2/v2.0/token";

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
                Console.WriteLine($"❌ Token ophalen mislukt: {response.StatusCode} - {error}");
                return null;
            }

            var responseContent = await response.Content.ReadAsStringAsync();
            var jsonDoc = JsonDocument.Parse(responseContent);
            return jsonDoc.RootElement.GetProperty("access_token").GetString();
        }
    }
}
