using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using ClusterClient.Clients;
using log4net;

namespace ClusterTests;

public class SmartClusterClient : ClusterClientBase
{
    public SmartClusterClient(string[] replicaAddresses)
        : base(replicaAddresses)
    {
    }

    public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
    {
        var tasks = new HashSet<Task<string>>();
        var replicaTimeout = timeout / ReplicaAddresses.Length;
        var sw = new Stopwatch();

        for (var i = 0; i < ReplicaAddresses.Length; i++)
        {
            var task = ProcessRequestAsync(CreateRequest($"{ReplicaAddresses[i]}?query={query}"));
            tasks.Add(task);
            
            sw.Start();
            await Task.WhenAny(Task.WhenAny(tasks), Task.Delay(replicaTimeout));
            sw.Stop();

            if (task.IsCompletedSuccessfully) 
                return await task;

            if (!task.IsFaulted) 
                continue;
            replicaTimeout += (replicaTimeout - sw.Elapsed) / Math.Max(1, ReplicaAddresses.Length - i - 1);
            tasks.Remove(task);
        }
        
        var finishedTask = tasks.FirstOrDefault(t => t.IsCompleted);
        if (finishedTask is not null)
            return await finishedTask;

        throw new TimeoutException();
    }

    protected override ILog Log => LogManager.GetLogger(typeof(SmartClusterClient));
}