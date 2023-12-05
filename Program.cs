using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using DotNext.Buffers.Text;
using DotNext.Net;
using DotNext.Net.Cluster;
using DotNext.Net.Cluster.Consensus.Raft;
using DotNext.Net.Cluster.Consensus.Raft.Membership;
using Microsoft.AspNetCore.Mvc;
using RaftNode;

namespace Clustering;

public static class Program
{
    public static async Task Main(string[] args)
    {
        var port = int.Parse(args[0]);

        // using var cluster = new Cluster();
        // var me = await cluster.Join();
        var loggerFactory = LoggerFactory.Create(ConfigureLogging);
        var config = new RaftCluster.TcpConfiguration(new IPEndPoint(IPAddress.Loopback, port))
        {
            RequestTimeout = TimeSpan.FromMilliseconds(140),
            LowerElectionTimeout = 150,
            UpperElectionTimeout = 300,
            TransmissionBlockSize = 4096,
            ColdStart = args.Length > 1 && args[1] == "true",
            LoggerFactory = loggerFactory,
            SslOptions = null
        };

        AddMembersToCluster(config.UseInMemoryConfigurationStorage());

        await using var cluster = new RaftCluster(config);

        var state = new SimplePersistentState($"{port}");
        cluster.AuditTrail = state;

        var builder = WebApplication.CreateBuilder(args);
        builder.WebHost.UseKestrel(f => { f.ListenLocalhost(int.Parse(args[0]) - 1000); });

        builder.Services.AddSingleton<IRaftCluster>(cluster);

        using var app = builder.Build();

        app.MapGet("/cluster",
            static ([FromServices] IRaftCluster cluster) => Results.Ok(string.Join(',', cluster.Members.Select(f => f.Id.ToString() + "/" + f.Status))));
        app.MapGet("/leader", async () => Results.Ok(cluster.Leader?.EndPoint.ToString()));

        app.MapPost("/cluster", async ([FromBody] AddMember member) =>
        {
            var address = IPAddress.Parse(member.Address);
            await cluster.AddMemberAsync(new IPEndPoint(address, member.Port)).ConfigureAwait(false);

            return Results.StatusCode(StatusCodes.Status200OK);
        });

        app.MapPost("/update", async () =>
        {
            if (!cluster.LeadershipToken.IsCancellationRequested)
            {
                try
                {
                    var entry = new Int64LogEntry { Content = Environment.TickCount64, Term = cluster.Term };
                    await cluster.ReplicateAsync(entry, CancellationToken.None);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Unexpected error {0}", e);
                }
            }
        });

        cluster.LeaderChanged += LeaderChanged;
        cluster.MemberRemoved += async (raftCluster, eventArgs) =>
        {
            Console.WriteLine("MemberRemoved {0} {1}", eventArgs.Member.EndPoint, eventArgs.Member.IsLeader);
        };

        try
        {
            await cluster.StartAsync(CancellationToken.None);
            await app.RunAsync();
        }
        finally
        {
            await cluster.StopAsync(CancellationToken.None);
        }

        return;


        static void AddMembersToCluster(InMemoryClusterConfigurationStorage<EndPoint> storage)
        {
            var builder = storage.CreateActiveConfigurationBuilder();

            builder.Add(new IPEndPoint(IPAddress.Loopback, 9000));
            builder.Add(new IPEndPoint(IPAddress.Loopback, 9001));
            builder.Add(new IPEndPoint(IPAddress.Loopback, 9002));

            builder.Build();
        }
    }

    static void ConfigureLogging(ILoggingBuilder builder)
        => builder.AddConsole().SetMinimumLevel(LogLevel.Trace);

    internal static void LeaderChanged(ICluster cluster, IClusterMember? leader)
    {
        Debug.Assert(cluster is IRaftCluster);
        var term = ((IRaftCluster)cluster).Term;
        var timeout = ((IRaftCluster)cluster).ElectionTimeout;
        Console.WriteLine(leader is null
            ? "Consensus cannot be reached"
            : $"New cluster leader is elected. Leader address is {leader.EndPoint}");
        Console.WriteLine($"Term of local cluster member is {term}. Election timeout {timeout}");
    }
}