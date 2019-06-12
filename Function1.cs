using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using MoreLinq;

namespace PCDurableFanOut
{
    public static class Function1

    {

        [FunctionName(nameof(Start_O_MSTest))]
        public static async Task<HttpResponseMessage> Start_O_MSTest([HttpTrigger("POST", Route = "start")] HttpRequestMessage req, [OrchestrationClient]DurableOrchestrationClient starter, ILogger log)
        {
            var stopWatch = new Stopwatch();
            stopWatch.Start();

            var len = 300;

            string instanceId = await starter.StartNewAsync(nameof(Orchestrator), len);

            stopWatch.Stop();
            log.LogInformation($"********Start_O_MSTest started in {stopWatch.ElapsedMilliseconds}ms for {len} items********");

            return starter.CreateCheckStatusResponse(req, instanceId);
       }

        [FunctionName(nameof(Orchestrator))]
        public static async Task Orchestrator([OrchestrationTrigger] DurableOrchestrationContext context, ILogger log)
       {

            log.LogInformation($"Orchestrator running");

            var start = context.CurrentUtcDateTime;
            var numItems = context.GetInput<int>();

            // call the splitter once so the orchestrator stores this value intead of rerunning it multiple times
            var workItems = await context.CallActivityAsync<IEnumerable<WorkItemBatch>>(nameof(Splitter), numItems);

            var tasks = workItems
                .Select(x =>
                {
                    return context.CallActivityAsync<IEnumerable<int>>(nameof(Worker), x);
                });
            var workersResult = await Task.WhenAll(tasks);

            var results = workersResult.Flatten();

            log.LogInformation($"Results compiled {results.Count()} items processed");

            var end = context.CurrentUtcDateTime;
            log.LogInformation($"********O_MSTest finished in {(end - start).TotalMilliseconds}ms for {numItems} items********");
        }

        // Generates work item batches from the input parameters
        [FunctionName(nameof(Splitter))]
        public static IEnumerable<WorkItemBatch> Splitter([ActivityTrigger]int numItems, ILogger log)
        {
            const int batchLimit = 50;
            int curWorker = 0;
            var list = Enumerable.Range(1, numItems).ToList();
            var tasks = list
                .Batch(batchLimit)
                .Select(i => new WorkItemBatch(i, curWorker++));

            return tasks;
        }

        // Processes a work item batch and returns a result array
        [FunctionName(nameof(Worker))]
        public static async Task<IEnumerable<int>> Worker([ActivityTrigger]WorkItemBatch workItemBatch, ILogger log)
        {
            log.LogInformation($"Worker {workItemBatch.workerNumber} started");
            const int progressSteps = 10;
            int progressCount = 0;
            var list = new List<Task>();
            var rand = new Random();
            var resultList = new List<int>();
            // each worker processes the tasks sequentially not in parallel because it's likely CPU bound or limtied by back-end resoure capacity
            // we're already running multiple workers in parallel -- that's our parallelism (we don't want two levels of parallelism)
            foreach (var item in workItemBatch.items)
            {
                int len = rand.Next(100, 500);  // ms (wall clock time) each work item takes to process
                await Task.Delay(len);
                resultList.Add(len);
                if (progressCount++ % progressSteps == 0)
                {
                    log.LogInformation($"Worker {workItemBatch.workerNumber} progress { progressCount } of {workItemBatch.items.Count()}");
                }
            }

            log.LogInformation($"Worker {workItemBatch.workerNumber} complete");

            return resultList;
        }

        public struct WorkItemBatch
        {
            public WorkItemBatch(IEnumerable<int> items, int workerNumber)
            {
                this.items = items;
                this.workerNumber = workerNumber;
            }

            public IEnumerable<int> items;
            public int workerNumber;
        }
    }
}