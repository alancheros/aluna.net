using Aluna.Abstractions;
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace Aluna.Repositories
{
    public class TimedRehydrationCordinator<TRepo, TAgg> : IRehydrationCoordinator<TRepo, TAgg>, IAsyncDisposable
        where TRepo : IRepository<TAgg> where TAgg : AggregateRoot
    {
        private readonly TimeSpan _triggerInterval;
        private readonly ConcurrentDictionary<TRepo, RepositoryPipeline> _pipelines = new();
        private readonly CancellationTokenSource _shutdown = new();

        public TimedRehydrationCordinator(TimeSpan? triggerInterval = null)
        {
            _triggerInterval = triggerInterval ?? TimeSpan.FromSeconds(5);
        }

        public Task TriggerAsync()
        {
            foreach (var pipeline in _pipelines.Values)
            {
                pipeline.TriggerChannel.Writer.TryWrite(true);
            }

            return Task.CompletedTask;
        }

        public void AddRepository(TRepo repository)
        {
            ArgumentNullException.ThrowIfNull(repository);

            if (_pipelines.ContainsKey(repository))
            {
                return;
            }

            var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_shutdown.Token);

            var channel = Channel.CreateBounded<bool>(new BoundedChannelOptions(1)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.DropOldest
            });

            var pipeline = new RepositoryPipeline(repository, channel, linkedCts);

            if (!_pipelines.TryAdd(repository, pipeline))
            {
                linkedCts.Dispose();
                return;
            }

            pipeline.TimerTask = Task.Run(() => RunTimerAsync(pipeline), linkedCts.Token);
            pipeline.WorkerTask = Task.Run(() => RunWorkerAsync(pipeline), linkedCts.Token);

            // Trigger initial hydration immediately.
            channel.Writer.TryWrite(true);
        }

        public async ValueTask DisposeAsync()
        {
            _shutdown.Cancel();

            var tasks = new List<Task>(_pipelines.Count * 2);
            foreach (var pipeline in _pipelines.Values)
            {
                pipeline.Cancellation.Cancel();

                if (pipeline.TimerTask is not null) tasks.Add(pipeline.TimerTask);
                if (pipeline.WorkerTask is not null) tasks.Add(pipeline.WorkerTask);
            }

            try
            {
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown.
            }

            foreach (var pipeline in _pipelines.Values)
            {
                pipeline.Cancellation.Dispose();
            }

            _shutdown.Dispose();
        }

        private async Task RunTimerAsync(RepositoryPipeline pipeline)
        {
            try
            {
                using var timer = new PeriodicTimer(_triggerInterval);

                while (await timer.WaitForNextTickAsync(pipeline.Cancellation.Token).ConfigureAwait(false))
                {
                    pipeline.TriggerChannel.Writer.TryWrite(true);
                }
            }
            catch (OperationCanceledException) when (pipeline.Cancellation.IsCancellationRequested)
            {
                // Expected on shutdown.
            }
            finally
            {
                pipeline.TriggerChannel.Writer.TryComplete();
            }
        }

        private static async Task RunWorkerAsync(RepositoryPipeline pipeline)
        {
            var token = pipeline.Cancellation.Token;
            var reader = pipeline.TriggerChannel.Reader;

            try
            {
                while (await reader.WaitToReadAsync(token).ConfigureAwait(false))
                {
                    while (reader.TryRead(out _))
                    {
                        var summary = await pipeline.Repository
                            .RehydrateFromStoreAsync(token)
                            .ConfigureAwait(false);

                        pipeline.LastScannedStoreIndex = Math.Max(
                            pipeline.LastScannedStoreIndex,
                            summary.LastScannedStoreIndex);
                    }
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                // Expected on shutdown.
            }
        }

        private sealed class RepositoryPipeline
        {
            public TRepo Repository { get; }
            public Channel<bool> TriggerChannel { get; }
            public CancellationTokenSource Cancellation { get; }
            public Task? TimerTask { get; set; }
            public Task? WorkerTask { get; set; }
            public long LastScannedStoreIndex { get; set; } = -1;

            public RepositoryPipeline(
                TRepo repository,
                Channel<bool> triggerChannel,
                CancellationTokenSource cancellation)
            {
                Repository = repository;
                TriggerChannel = triggerChannel;
                Cancellation = cancellation;
            }
        }
    }
}
