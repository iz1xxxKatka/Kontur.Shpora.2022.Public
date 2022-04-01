using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using ClusterClient.Clients;
using log4net;

namespace ClusterTests;

public class RoundRobinClusterClient : ClusterClientBase
{
    public RoundRobinClusterClient(string[] replicaAddresses)
        : base(replicaAddresses)
    {
    }

    public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
    {
        var replicaTimeout = timeout / ReplicaAddresses.Length;

        var sw = new Stopwatch();

        for (var i = 0; i < ReplicaAddresses.Length; i++)
        {
            var task = ProcessRequestAsync(CreateRequest($"{ReplicaAddresses[i]}?query={query}"));
            sw.Start();
            await Task.WhenAny(task, Task.Delay(replicaTimeout));
            sw.Stop();
            if (task.IsCompletedSuccessfully)
                return await task;
            if (task.IsFaulted)
                replicaTimeout += (replicaTimeout - sw.Elapsed) / Math.Max(1, ReplicaAddresses.Length - i - 1);
        }

        throw new TimeoutException();
    }

    protected override ILog Log => LogManager.GetLogger(typeof(RoundRobinClusterClient));
}