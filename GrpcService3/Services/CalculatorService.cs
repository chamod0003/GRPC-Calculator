using Grpc.Core;
using GrpcCalculator;

namespace GrpcService3.Services // Change namespace for each service
{
    public class CalculatorService : Calculator.CalculatorBase
    {
        private readonly ILogger<CalculatorService> _logger;
        private readonly string _serverName;
        private readonly VectorClock _vectorClock;
        private readonly List<VectorClockEvent> _eventLog;
        private readonly object _logLock = new object();
        private readonly DateTime _startTime;
        private int _requestCount = 0;

        public CalculatorService(ILogger<CalculatorService> logger, IConfiguration configuration)
        {
            _logger = logger;
            _serverName = configuration["ServerName"] ?? "Server";
            _startTime = DateTime.Now;

            // Initialize vector clock with all process IDs
            var processIds = new List<string> { "Client", "Server1", "Server2", "Server3" };
            _vectorClock = new VectorClock(_serverName.Replace(" ", ""), processIds);
            _eventLog = new List<VectorClockEvent>();

            LogEvent("SERVER_START", "Server initialization");

            _logger.LogInformation("═══════════════════════════════════════════════════");
            _logger.LogInformation($"🚀 {_serverName} STARTED");
            _logger.LogInformation($"📍 Service: Distributed Calculator (Professional VC)");
            _logger.LogInformation($"⏰ Time: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            _logger.LogInformation($"🕐 Initial Vector Clock: {_vectorClock}");
            _logger.LogInformation("═══════════════════════════════════════════════════");
        }

        public override Task<HealthCheckReply> HealthCheck(HealthCheckRequest request, ServerCallContext context)
        {
            var uptime = (long)(DateTime.Now - _startTime).TotalSeconds;

            _vectorClock.Increment();
            LogEvent("HEALTH_CHECK", $"Health check from {request.ClientId}");

            _logger.LogInformation($"💓 Health check from {request.ClientId} | VC: {_vectorClock} | Uptime: {uptime}s | Requests: {_requestCount}");

            return Task.FromResult(new HealthCheckReply
            {
                IsHealthy = true,
                ServerName = _serverName,
                UptimeSeconds = uptime
            });
        }

        public override Task<SumReply> CalculatePartialSum(PartialSumRequest request, ServerCallContext context)
        {
            _requestCount++;

            var receivedClock = request.VectorClock.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            var beforeClock = _vectorClock.Clone();

            // Update vector clock with received message
            _vectorClock.Update(receivedClock);

            LogEvent("REQUEST_RECEIVED",
                $"Partial sum request [{request.Start}-{request.End}] from RequestID: {request.RequestId}");

            // Determine causality relationship
            string causalityInfo = AnalyzeCausality(receivedClock, beforeClock);

            _logger.LogInformation("╔════════════════════════════════════════════════════════════╗");
            _logger.LogInformation($"║ 📨 PARTIAL SUM REQUEST #{_requestCount}");
            _logger.LogInformation($"║ 🆔 Request ID: {request.RequestId}");
            _logger.LogInformation($"║ ⏰ Timestamp: {DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}");
            _logger.LogInformation($"║ 📊 Range: {request.Start} to {request.End}");
            _logger.LogInformation($"║ 🔢 Numbers to process: {request.End - request.Start + 1}");
            _logger.LogInformation("║ ════════════════════════════════════════════════════════");
            _logger.LogInformation("║ 🕐 VECTOR CLOCK ANALYSIS:");
            _logger.LogInformation($"║    📥 Received VC: {FormatVectorClock(receivedClock)}");
            _logger.LogInformation($"║    📍 Before Update: {beforeClock}");
            _logger.LogInformation($"║    📤 After Update:  {_vectorClock}");
            _logger.LogInformation($"║    🔗 Causality: {causalityInfo}");
            _logger.LogInformation("╚════════════════════════════════════════════════════════════╝");

            // Calculate partial sum
            long sumToEnd = (long)request.End * (request.End + 1) / 2;
            long sumToStartMinus1 = request.Start > 1
                ? (long)(request.Start - 1) * request.Start / 2
                : 0;
            long partialSum = sumToEnd - sumToStartMinus1;

            // Simulate processing
            var processingTime = (request.End - request.Start + 1) / 10;
            Thread.Sleep(Math.Min(processingTime, 100));

            LogEvent("CALCULATION_COMPLETE",
                $"Calculated sum [{request.Start}-{request.End}] = {partialSum}");

            _logger.LogInformation("╔════════════════════════════════════════════════════════════╗");
            _logger.LogInformation($"║ ✅ CALCULATION COMPLETE");
            _logger.LogInformation($"║ 📊 Partial Sum ({request.Start}-{request.End}): {partialSum}");
            _logger.LogInformation($"║ ⚡ Processing time: ~{processingTime}ms");
            _logger.LogInformation($"║ 🕐 Final VC: {_vectorClock}");
            _logger.LogInformation($"║ 📈 Total requests: {_requestCount} | Events logged: {_eventLog.Count}");
            _logger.LogInformation("╚════════════════════════════════════════════════════════════╝");
            _logger.LogInformation("");

            var reply = new SumReply
            {
                PartialSum = partialSum,
                ServerName = _serverName,
                Timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"),
                RangeStart = request.Start,
                RangeEnd = request.End,
                RequestId = request.RequestId
            };

            var currentClock = _vectorClock.GetClock();
            foreach (var kvp in currentClock)
            {
                reply.VectorClock.Add(kvp.Key, kvp.Value);
            }

            return Task.FromResult(reply);
        }

        private void LogEvent(string eventType, string description)
        {
            lock (_logLock)
            {
                var evt = new VectorClockEvent
                {
                    EventId = Guid.NewGuid().ToString().Substring(0, 8),
                    ProcessId = _serverName.Replace(" ", ""),
                    EventType = eventType,
                    VectorClock = _vectorClock.GetClock(),
                    Timestamp = DateTime.Now,
                    Description = description
                };
                _eventLog.Add(evt);
            }
        }

        private string AnalyzeCausality(Dictionary<string, int> receivedClock, VectorClock beforeClock)
        {
            var comparison = beforeClock.CompareTo(receivedClock);

            if (comparison < 0)
            {
                return "📍 CAUSALLY AFTER (Received event happened after local state)";
            }
            else if (comparison > 0)
            {
                return "📍 CAUSALLY BEFORE (Local state is ahead of received event)";
            }
            else
            {
                return "⚡ CONCURRENT (Events happened independently)";
            }
        }

        private string FormatVectorClock(Dictionary<string, int> clock)
        {
            return "{" + string.Join(", ", clock.OrderBy(k => k.Key)
                .Select(kvp => $"{kvp.Key}:{kvp.Value}")) + "}";
        }
    }
}