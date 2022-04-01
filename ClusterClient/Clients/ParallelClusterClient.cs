using System;
using System.Linq;
using System.Threading.Tasks;
using ClusterClient.Clients;
using log4net;

namespace ClusterTests;

public class ParallelClusterClient : ClusterClientBase
{
    public ParallelClusterClient(string[] replicaAddresses)
        : base(replicaAddresses)
    {
    }

    public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
    {
        var tasks = ReplicaAddresses.Select(
            x => ProcessRequestAsync(CreateRequest($"{x}?query={query}"))
        ).ToHashSet();
        do
        {
            var finishedTask = await Task.WhenAny(tasks).WaitAsync(timeout);
            tasks.Remove(finishedTask);
            if (finishedTask.IsCompletedSuccessfully || tasks.Count == 0)
                return await finishedTask;
        } while (true);
    }

    protected override ILog Log => LogManager.GetLogger(typeof(ParallelClusterClient));
}