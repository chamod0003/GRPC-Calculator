using Grpc.Net.Client;
using GrpcCalculator;
using Grpc.Core;
using System.Diagnostics;

Console.WriteLine("╔══════════════════════════════════════════════════════════════╗");
Console.WriteLine("║   DISTRIBUTED CALCULATOR - PROFESSIONAL VECTOR CLOCK         ║");
Console.WriteLine("║   With Causality Tracking & Concurrent Event Detection       ║");
Console.WriteLine("╚══════════════════════════════════════════════════════════════╝");
Console.WriteLine();

var servers = new List<ServerInfo>
{
    new ServerInfo { Id = 1, Name = "Server1", Address = "http://localhost:5001" },
    new ServerInfo { Id = 2, Name = "Server2", Address = "http://localhost:5002" },
    new ServerInfo { Id = 3, Name = "Server3", Address = "http://localhost:5003" }
};

// Initialize professional vector clock
var processIds = new List<string> { "Client", "Server1", "Server2", "Server3" };
var vectorClock = new VectorClock("Client", processIds);
var eventLog = new List<VectorClockEvent>();

string clientId = $"Client-{Guid.NewGuid().ToString().Substring(0, 8)}";

LogClientEvent("CLIENT_START", "Client application started");

Console.WriteLine($"🆔 Client ID: {clientId}");
Console.WriteLine($"🕐 Initial Vector Clock: {vectorClock}\n");

while (true)
{
    try
    {
        Console.WriteLine("\n┌──────────────────────────────────────────────────────────┐");
        Console.WriteLine("│ OPTIONS:                                                 │");
        Console.WriteLine("│ 1. Calculate with Auto Load Balancing (All Servers)     │");
        Console.WriteLine("│ 2. Calculate with Manual Server Selection               │");
        Console.WriteLine("│ 3. Check Server Health                                   │");
        Console.WriteLine("│ 4. Show Vector Clock State & Analysis                   │");
        Console.WriteLine("│ 5. Show Event Log with Causality                        │");
        Console.WriteLine("│ 0. Exit                                                  │");
        Console.WriteLine("└──────────────────────────────────────────────────────────┘");
        Console.Write("\n👉 Select option: ");

        var choice = Console.ReadLine();

        if (choice == "0")
        {
            Console.WriteLine("\n👋 Exiting...");
            LogClientEvent("CLIENT_STOP", "Client application stopped");
            break;
        }

        switch (choice)
        {
            case "1":
                await CalculateWithAutoLoadBalancing();
                break;
            case "2":
                await CalculateWithManualSelection();
                break;
            case "3":
                await CheckServerHealth();
                break;
            case "4":
                DisplayVectorClockAnalysis();
                break;
            case "5":
                DisplayEventLog();
                break;
            default:
                Console.WriteLine("❌ Invalid selection.");
                break;
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"\n❌ Unexpected Error: {ex.Message}");
    }
}

void LogClientEvent(string eventType, string description)
{
    var evt = new VectorClockEvent
    {
        EventId = Guid.NewGuid().ToString().Substring(0, 8),
        ProcessId = "Client",
        EventType = eventType,
        VectorClock = vectorClock.GetClock(),
        Timestamp = DateTime.Now,
        Description = description
    };
    eventLog.Add(evt);
}

async Task CalculateWithAutoLoadBalancing()
{
    Console.Write("\n🔢 Enter a number (n) to calculate sum from 1 to n: ");
    var input = Console.ReadLine();

    if (!int.TryParse(input, out int n) || n < 1)
    {
        Console.WriteLine("❌ Invalid number. Please enter a positive integer.");
        return;
    }

    vectorClock.Increment();
    LogClientEvent("REQUEST_INIT", $"Initiating calculation for n={n} (Auto mode)");

    string requestId = Guid.NewGuid().ToString().Substring(0, 8);

    Console.WriteLine("\n╔══════════════════════════════════════════════════════════════╗");
    Console.WriteLine("║ 🔄 AUTO MODE - CHECKING ALL SERVERS                          ║");
    Console.WriteLine($"║ 🕐 Current VC: {vectorClock,-44} ║");
    Console.WriteLine("╚══════════════════════════════════════════════════════════════╝");

    var availableServers = await GetAvailableServers();

    if (availableServers.Count == 0)
    {
        Console.WriteLine("\n❌ No servers available! Please start at least one server.");
        return;
    }

    await ProcessDistributedCalculation(n, availableServers, requestId);
}

async Task CalculateWithManualSelection()
{
    Console.WriteLine("\n╔══════════════════════════════════════════════════════════════╗");
    Console.WriteLine("║ 🎯 MANUAL SERVER SELECTION MODE                              ║");
    Console.WriteLine("╚══════════════════════════════════════════════════════════════╝");

    var serverStatus = new Dictionary<int, bool>();

    Console.WriteLine("\n📡 Server Status:");
    foreach (var server in servers)
    {
        bool isAvailable = await IsServerAvailable(server);
        serverStatus[server.Id] = isAvailable;
        string status = isAvailable ? "✅ ONLINE" : "❌ OFFLINE";
        Console.WriteLine($"   {server.Id}. {server.Name,-12} ({server.Address}) - {status}");
    }

    Console.WriteLine("\n💡 Select servers to use (comma-separated, e.g., 1,2 or 1,3)");
    Console.Write("👉 Your selection: ");
    var selection = Console.ReadLine();

    var selectedIds = new List<int>();
    try
    {
        selectedIds = selection.Split(',')
            .Select(s => int.Parse(s.Trim()))
            .Where(id => id >= 1 && id <= 3)
            .Distinct()
            .ToList();
    }
    catch
    {
        Console.WriteLine("❌ Invalid selection format.");
        return;
    }

    if (selectedIds.Count == 0)
    {
        Console.WriteLine("❌ No valid servers selected.");
        return;
    }

    var selectedServers = servers.Where(s => selectedIds.Contains(s.Id)).ToList();
    var availableSelected = selectedServers.Where(s => serverStatus[s.Id]).ToList();
    var unavailableSelected = selectedServers.Where(s => !serverStatus[s.Id]).ToList();

    if (unavailableSelected.Count > 0)
    {
        Console.WriteLine("\n⚠️  Warning: Some selected servers are offline:");
        foreach (var server in unavailableSelected)
        {
            Console.WriteLine($"   └─ {server.Name}");
        }
    }

    if (availableSelected.Count == 0)
    {
        Console.WriteLine("\n❌ None of the selected servers are available!");
        return;
    }

    Console.Write("\n🔢 Enter a number (n): ");
    var input = Console.ReadLine();

    if (!int.TryParse(input, out int n) || n < 1)
    {
        Console.WriteLine("❌ Invalid number.");
        return;
    }

    vectorClock.Increment();
    LogClientEvent("REQUEST_INIT", $"Initiating calculation for n={n} (Manual: {string.Join(",", selectedIds)})");

    string requestId = Guid.NewGuid().ToString().Substring(0, 8);

    Console.WriteLine("\n╔══════════════════════════════════════════════════════════════╗");
    Console.WriteLine("║ 🎯 MANUAL MODE - USING SELECTED SERVERS                      ║");
    Console.WriteLine($"║ 🕐 Current VC: {vectorClock,-44} ║");
    Console.WriteLine("╚══════════════════════════════════════════════════════════════╝");

    await ProcessDistributedCalculation(n, availableSelected, requestId);
}

async Task ProcessDistributedCalculation(int n, List<ServerInfo> serversToUse, string requestId)
{
    Console.WriteLine($"\n✅ Using {serversToUse.Count} server(s):");
    foreach (var server in serversToUse)
    {
        Console.WriteLine($"   └─ {server.Name} ({server.Address})");
    }

    var ranges = DivideWork(n, serversToUse.Count);
    var tasks = new List<Task<SumReply>>();
    var stopwatch = Stopwatch.StartNew();
    var requestClocks = new Dictionary<string, Dictionary<string, int>>();

    Console.WriteLine($"\n📊 Load Distribution:");
    for (int i = 0; i < serversToUse.Count; i++)
    {
        var server = serversToUse[i];
        var range = ranges[i];
        int numbersToProcess = range.End - range.Start + 1;
        double percentage = (numbersToProcess * 100.0) / n;

        // Store the clock state for each request
        requestClocks[server.Name] = vectorClock.GetClock();

        Console.WriteLine($"   └─ {server.Name,-12}: [{range.Start,6}-{range.End,6}] = {numbersToProcess,6} nums ({percentage:F1}%) | VC: {vectorClock}");

        tasks.Add(CalculatePartialSumAsync(server, range.Start, range.End, requestId));
    }

    Console.WriteLine("\n⏳ Processing requests in parallel...\n");

    var results = await Task.WhenAll(tasks);
    stopwatch.Stop();

    var successfulResults = results.Where(r => r != null).ToList();

    if (successfulResults.Count == 0)
    {
        Console.WriteLine("❌ All servers failed!");
        return;
    }

    long totalSum = 0;
    Console.WriteLine("╔══════════════════════════════════════════════════════════════╗");
    Console.WriteLine("║ 📥 RESULTS WITH VECTOR CLOCK ANALYSIS                        ║");
    Console.WriteLine("╚══════════════════════════════════════════════════════════════╝");

    foreach (var result in successfulResults.OrderBy(r => r.RangeStart))
    {
        totalSum += result.PartialSum;
        var receivedClock = result.VectorClock.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        var sentClock = requestClocks.ContainsKey(result.ServerName)
            ? requestClocks[result.ServerName]
            : new Dictionary<string, int>();

        string causalityInfo = AnalyzeCausality(sentClock, receivedClock);

        Console.WriteLine($"\n🔹 {result.ServerName}:");
        Console.WriteLine($"   Range: [{result.RangeStart}-{result.RangeEnd}] | Sum: {result.PartialSum}");
        Console.WriteLine($"   Sent VC:     {FormatVC(sentClock)}");
        Console.WriteLine($"   Received VC: {FormatVC(receivedClock)}");
        Console.WriteLine($"   Causality: {causalityInfo}");

        vectorClock.Update(receivedClock);
    }

    LogClientEvent("CALCULATION_COMPLETE", $"Completed calculation for n={n}, result={totalSum}");

    Console.WriteLine("\n╔══════════════════════════════════════════════════════════════╗");
    Console.WriteLine("║ ✅ FINAL RESULT                                              ║");
    Console.WriteLine("╠══════════════════════════════════════════════════════════════╣");
    Console.WriteLine($"║ 📊 Total Sum (1 to {n}): {totalSum}");
    Console.WriteLine($"║ ⏱️  Total Time: {stopwatch.ElapsedMilliseconds} ms");
    Console.WriteLine($"║ 🖥️  Servers: {successfulResults.Count}/{serversToUse.Count}");
    Console.WriteLine($"║ 🆔 Request: {requestId}");
    Console.WriteLine($"║ 🕐 Final VC: {vectorClock}");
    Console.WriteLine("╚══════════════════════════════════════════════════════════════╝");

    long expectedSum = (long)n * (n + 1) / 2;
    Console.WriteLine(totalSum == expectedSum
        ? "✅ Verification: CORRECT!"
        : $"⚠️ Expected {expectedSum}, got {totalSum}");
}

async Task<SumReply> CalculatePartialSumAsync(ServerInfo server, int start, int end, string requestId)
{
    try
    {
        using var channel = GrpcChannel.ForAddress(server.Address);
        var client = new Calculator.CalculatorClient(channel);

        var request = new PartialSumRequest
        {
            Start = start,
            End = end,
            RequestId = requestId
        };

        var currentClock = vectorClock.GetClock();
        foreach (var kvp in currentClock)
        {
            request.VectorClock.Add(kvp.Key, kvp.Value);
        }

        var reply = await client.CalculatePartialSumAsync(request,
            deadline: DateTime.UtcNow.AddSeconds(10));

        return reply;
    }
    catch (Exception ex)
    {
        Console.WriteLine($"❌ {server.Name}: {ex.Message}");
        return null;
    }
}

async Task<bool> IsServerAvailable(ServerInfo server)
{
    try
    {
        using var channel = GrpcChannel.ForAddress(server.Address);
        var client = new Calculator.CalculatorClient(channel);

        var healthReply = await client.HealthCheckAsync(
            new HealthCheckRequest { ClientId = clientId },
            deadline: DateTime.UtcNow.AddSeconds(1));

        return healthReply.IsHealthy;
    }
    catch
    {
        return false;
    }
}

async Task<List<ServerInfo>> GetAvailableServers()
{
    var available = new List<ServerInfo>();

    foreach (var server in servers)
    {
        if (await IsServerAvailable(server))
        {
            available.Add(server);
        }
    }

    return available;
}

List<(int Start, int End)> DivideWork(int n, int serverCount)
{
    var ranges = new List<(int Start, int End)>();
    int numbersPerServer = n / serverCount;
    int remainder = n % serverCount;
    int currentStart = 1;

    for (int i = 0; i < serverCount; i++)
    {
        int rangeSize = numbersPerServer + (i < remainder ? 1 : 0);
        int currentEnd = currentStart + rangeSize - 1;
        ranges.Add((currentStart, currentEnd));
        currentStart = currentEnd + 1;
    }

    return ranges;
}

async Task CheckServerHealth()
{
    vectorClock.Increment();
    LogClientEvent("HEALTH_CHECK", "Checking all servers");

    Console.WriteLine("\n╔══════════════════════════════════════════════════════════════╗");
    Console.WriteLine("║ 💓 SERVER HEALTH CHECK                                       ║");
    Console.WriteLine($"║ 🕐 Current VC: {vectorClock,-44} ║");
    Console.WriteLine("╚══════════════════════════════════════════════════════════════╝\n");

    foreach (var server in servers)
    {
        try
        {
            using var channel = GrpcChannel.ForAddress(server.Address);
            var client = new Calculator.CalculatorClient(channel);

            var healthReply = await client.HealthCheckAsync(
                new HealthCheckRequest { ClientId = clientId },
                deadline: DateTime.UtcNow.AddSeconds(2));

            if (healthReply.IsHealthy)
            {
                Console.WriteLine($"✅ {server.Name,-12} | HEALTHY | Uptime: {healthReply.UptimeSeconds}s");
            }
            else
            {
                Console.WriteLine($"⚠️ {server.Name,-12} | UNHEALTHY");
            }
        }
        catch
        {
            Console.WriteLine($"❌ {server.Name,-12} | DOWN");
        }
    }
}

void DisplayVectorClockAnalysis()
{
    Console.WriteLine("\n╔══════════════════════════════════════════════════════════════╗");
    Console.WriteLine("║ 🕐 VECTOR CLOCK STATE & ANALYSIS                            ║");
    Console.WriteLine("╚══════════════════════════════════════════════════════════════╝");

    var currentClock = vectorClock.GetClock();

    Console.WriteLine("\n📊 Current Vector Clock State:");
    Console.WriteLine($"   {vectorClock}");

    Console.WriteLine("\n📈 Detailed Breakdown:");
    foreach (var kvp in currentClock.OrderBy(x => x.Key))
    {
        Console.WriteLine($"   └─ {kvp.Key,-15}: {kvp.Value,3} events");
    }

    Console.WriteLine("\n📚 Vector Clock Properties:");
    Console.WriteLine("   ✓ Causality Tracking: Enabled");
    Console.WriteLine("   ✓ Concurrent Detection: Active");
    Console.WriteLine($"   ✓ Total Events Logged: {eventLog.Count}");
    Console.WriteLine($"   ✓ Processes Tracked: {currentClock.Count}");

    int totalEvents = currentClock.Values.Sum();
    Console.WriteLine($"\n⚡ Total Logical Events: {totalEvents}");

    Console.WriteLine("\n💡 Explanation:");
    Console.WriteLine("   • Each process has a logical clock value");
    Console.WriteLine("   • Clock increments on local events");
    Console.WriteLine("   • Clocks merge on message receive (take max + increment)");
    Console.WriteLine("   • Used to determine: happened-before, concurrent, or after");

    // Show causality examples
    if (eventLog.Count >= 2)
    {
        Console.WriteLine("\n🔍 Recent Causality Analysis:");
        var recentEvents = eventLog.TakeLast(3).ToList();
        foreach (var evt in recentEvents)
        {
            Console.WriteLine($"   └─ [{evt.EventType}] at {evt.Timestamp:HH:mm:ss.fff} | VC: {FormatVC(evt.VectorClock)}");
        }
    }
}

void DisplayEventLog()
{
    Console.WriteLine("\n╔══════════════════════════════════════════════════════════════╗");
    Console.WriteLine("║ 📋 EVENT LOG WITH CAUSALITY INFORMATION                      ║");
    Console.WriteLine("╚══════════════════════════════════════════════════════════════╝\n");

    if (eventLog.Count == 0)
    {
        Console.WriteLine("   No events logged yet.");
        return;
    }

    Console.WriteLine($"📊 Total Events: {eventLog.Count}\n");

    // Group events by type
    var groupedEvents = eventLog.GroupBy(e => e.EventType).ToList();

    Console.WriteLine("📈 Event Type Summary:");
    foreach (var group in groupedEvents)
    {
        Console.WriteLine($"   └─ {group.Key,-20}: {group.Count(),3} events");
    }

    Console.WriteLine("\n📜 Event Timeline (Last 10 events):");
    Console.WriteLine("─────────────────────────────────────────────────────────────");

    var recentEvents = eventLog.TakeLast(10).ToList();
    for (int i = 0; i < recentEvents.Count; i++)
    {
        var evt = recentEvents[i];
        Console.WriteLine($"\n{i + 1}. [{evt.EventType}]");
        Console.WriteLine($"   Time: {evt.Timestamp:yyyy-MM-dd HH:mm:ss.fff}");
        Console.WriteLine($"   Process: {evt.ProcessId}");
        Console.WriteLine($"   Vector Clock: {FormatVC(evt.VectorClock)}");
        Console.WriteLine($"   Description: {evt.Description}");

        // Analyze causality with previous event
        if (i > 0)
        {
            var prevEvt = recentEvents[i - 1];
            string relationship = AnalyzeCausality(prevEvt.VectorClock, evt.VectorClock);
            Console.WriteLine($"   Relation to prev: {relationship}");
        }
    }

    Console.WriteLine("\n─────────────────────────────────────────────────────────────");
}

string AnalyzeCausality(Dictionary<string, int> clock1, Dictionary<string, int> clock2)
{
    bool allLessOrEqual = true;
    bool allGreaterOrEqual = true;
    bool anyLess = false;
    bool anyGreater = false;

    foreach (var key in clock1.Keys)
    {
        if (!clock2.ContainsKey(key)) continue;

        if (clock1[key] < clock2[key])
        {
            anyLess = true;
            allGreaterOrEqual = false;
        }
        else if (clock1[key] > clock2[key])
        {
            anyGreater = true;
            allLessOrEqual = false;
        }
    }

    if (allLessOrEqual && anyLess)
    {
        return "→ (Happened Before)";
    }
    else if (allGreaterOrEqual && anyGreater)
    {
        return "← (Happened After)";
    }
    else
    {
        return "|| (Concurrent)";
    }
}

string FormatVC(Dictionary<string, int> clock)
{
    return "{" + string.Join(", ", clock.OrderBy(k => k.Key)
        .Select(kvp => $"{kvp.Key}:{kvp.Value}")) + "}";
}

class ServerInfo
{
    public int Id { get; set; }
    public string Name { get; set; }
    public string Address { get; set; }
}