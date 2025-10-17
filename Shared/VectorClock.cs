using System.Text;

// Use the same namespace as proto files for consistency
namespace GrpcCalculator
{
    /// <summary>
    /// Professional Vector Clock implementation for distributed systems
    /// Tracks causality and detects concurrent events
    /// </summary>
    public class VectorClock
    {
        private readonly Dictionary<string, int> _clock;
        private readonly string _processId;
        private readonly object _lock = new object();

        public VectorClock(string processId, List<string> allProcessIds)
        {
            _processId = processId;
            _clock = new Dictionary<string, int>();

            foreach (var id in allProcessIds)
            {
                _clock[id] = 0;
            }
        }

        /// <summary>
        /// Increment local clock for this process
        /// </summary>
        public void Increment()
        {
            lock (_lock)
            {
                if (_clock.ContainsKey(_processId))
                {
                    _clock[_processId]++;
                }
            }
        }

        /// <summary>
        /// Update clock upon receiving a message (merge operation)
        /// </summary>
        public void Update(Dictionary<string, int> receivedClock)
        {
            lock (_lock)
            {
                foreach (var kvp in receivedClock)
                {
                    if (_clock.ContainsKey(kvp.Key))
                    {
                        _clock[kvp.Key] = Math.Max(_clock[kvp.Key], kvp.Value);
                    }
                }

                // Increment own clock after merge
                if (_clock.ContainsKey(_processId))
                {
                    _clock[_processId]++;
                }
            }
        }

        /// <summary>
        /// Check if this clock happened before another clock (causality)
        /// Returns true if this → other
        /// </summary>
        public bool HappenedBefore(Dictionary<string, int> otherClock)
        {
            lock (_lock)
            {
                bool anyLess = false;
                bool allLessOrEqual = true;

                foreach (var kvp in _clock)
                {
                    if (!otherClock.ContainsKey(kvp.Key))
                        continue;

                    if (kvp.Value < otherClock[kvp.Key])
                        anyLess = true;

                    if (kvp.Value > otherClock[kvp.Key])
                        allLessOrEqual = false;
                }

                return anyLess && allLessOrEqual;
            }
        }

        /// <summary>
        /// Check if two events are concurrent (no causal relationship)
        /// </summary>
        public bool IsConcurrentWith(Dictionary<string, int> otherClock)
        {
            lock (_lock)
            {
                bool thisBeforeOther = HappenedBefore(otherClock);
                bool otherBeforeThis = false;

                // Check if other happened before this
                bool anyLess = false;
                bool allLessOrEqual = true;

                foreach (var kvp in otherClock)
                {
                    if (!_clock.ContainsKey(kvp.Key))
                        continue;

                    if (kvp.Value < _clock[kvp.Key])
                        anyLess = true;

                    if (kvp.Value > _clock[kvp.Key])
                        allLessOrEqual = false;
                }

                otherBeforeThis = anyLess && allLessOrEqual;

                return !thisBeforeOther && !otherBeforeThis;
            }
        }

        /// <summary>
        /// Get current clock snapshot
        /// </summary>
        public Dictionary<string, int> GetClock()
        {
            lock (_lock)
            {
                return new Dictionary<string, int>(_clock);
            }
        }

        /// <summary>
        /// Get clock value for specific process
        /// </summary>
        public int GetClockValue(string processId)
        {
            lock (_lock)
            {
                return _clock.ContainsKey(processId) ? _clock[processId] : 0;
            }
        }

        /// <summary>
        /// Compare two vector clocks
        /// Returns: -1 if this < other, 1 if this > other, 0 if concurrent
        /// </summary>
        public int CompareTo(Dictionary<string, int> otherClock)
        {
            lock (_lock)
            {
                if (HappenedBefore(otherClock))
                    return -1;

                bool otherBeforeThis = false;
                bool anyLess = false;
                bool allLessOrEqual = true;

                foreach (var kvp in otherClock)
                {
                    if (!_clock.ContainsKey(kvp.Key))
                        continue;

                    if (kvp.Value < _clock[kvp.Key])
                        anyLess = true;

                    if (kvp.Value > _clock[kvp.Key])
                        allLessOrEqual = false;
                }

                otherBeforeThis = anyLess && allLessOrEqual;

                if (otherBeforeThis)
                    return 1;

                return 0; // Concurrent
            }
        }

        /// <summary>
        /// Get formatted string representation
        /// </summary>
        public string ToFormattedString()
        {
            lock (_lock)
            {
                var sb = new StringBuilder();
                sb.Append("{");
                var items = _clock.OrderBy(kvp => kvp.Key).ToList();
                for (int i = 0; i < items.Count; i++)
                {
                    sb.Append($"{items[i].Key}:{items[i].Value}");
                    if (i < items.Count - 1)
                        sb.Append(", ");
                }
                sb.Append("}");
                return sb.ToString();
            }
        }

        /// <summary>
        /// Create a copy of this vector clock
        /// </summary>
        public VectorClock Clone()
        {
            lock (_lock)
            {
                var clone = new VectorClock(_processId, _clock.Keys.ToList());
                foreach (var kvp in _clock)
                {
                    clone._clock[kvp.Key] = kvp.Value;
                }
                return clone;
            }
        }

        public override string ToString()
        {
            return ToFormattedString();
        }
    }

    /// <summary>
    /// Event logging with vector clock
    /// </summary>
    public class VectorClockEvent
    {
        public string EventId { get; set; }
        public string ProcessId { get; set; }
        public string EventType { get; set; }
        public Dictionary<string, int> VectorClock { get; set; }
        public DateTime Timestamp { get; set; }
        public string Description { get; set; }

        public string GetCausalityInfo()
        {
            var vc = string.Join(", ", VectorClock.OrderBy(k => k.Key)
                .Select(kvp => $"{kvp.Key}:{kvp.Value}"));
            return $"[{vc}]";
        }
    }
}