﻿using DotNext;
using DotNext.Net.Cluster.Consensus.Raft;

namespace RaftNode;

internal sealed class DataModifier : BackgroundService
{
    private readonly IRaftCluster cluster;
    private readonly ISupplier<long> valueProvider;

    public DataModifier(IRaftCluster cluster, ISupplier<long> provider)
    {
        this.cluster = cluster;
        valueProvider = provider;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
        {
            if (!cluster.LeadershipToken.IsCancellationRequested)
            {
                var newValue = valueProvider.Invoke() + 500L;
                Console.WriteLine("Saving value {0} generated by the leader node", newValue);

                try
                {
                    var entry = new Int64LogEntry { Content = newValue, Term = cluster.Term };
                    await cluster.ReplicateAsync(entry, stoppingToken);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Unexpected error {0}", e);
                }
            }
        }
    }
}