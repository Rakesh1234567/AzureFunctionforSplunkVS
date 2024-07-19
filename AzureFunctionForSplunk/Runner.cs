//
// AzureFunctionForSplunkVS
//
// Copyright (c) Microsoft Corporation
//
// All rights reserved. 
//
// MIT License
//
// Permission is hereby granted, free of charge, to any person obtaining a copy 
// of this software and associated documentation files (the ""Software""), to deal 
// in the Software without restriction, including without limitation the rights 
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
// copies of the Software, and to permit persons to whom the Software is furnished 
// to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all 
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS 
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR 
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER 
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION 
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//
using Microsoft.Azure.WebJobs;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Queue;
using System.Threading.Tasks;
using System.Collections.Generic;
using System;
using System.IO;
using Newtonsoft.Json;

namespace AzureFunctionForSplunk
{
    public class Runner
    {
        public async Task Run<T1, T2>(
            string[] messages,
            IBinder blobFaultBinder,
            Binder queueFaultBinder,
            IBinder incomingBatchBinder,
            IAsyncCollector<string> outputEvents,
            ILogger log)
        {
            var batchId = Guid.NewGuid().ToString();

            log.LogInformation($"[5064] :Before getEnvironmentVariable");
            bool logIncoming = Utils.getEnvironmentVariable("logIncomingBatches").ToLower() == "true";
            log.LogInformation($"[5064] :After getEnvironmentVariable");

            var azMonMsgs = (AzMonMessages)Activator.CreateInstance(typeof(T1), log);
            List<string> decomposed = null;
            log.LogInformation($"[5064] :Created azMonMsgs");
            try
            {
                log.LogInformation($"[5064] :Before DecomposeIncomingBatch");
                decomposed = azMonMsgs.DecomposeIncomingBatch(messages);
                log.LogInformation($"[5064] :After DecomposeIncomingBatch");
                if (logIncoming)
                {
                    try
                    {
                        var blobWriter = await incomingBatchBinder.BindAsync<CloudBlockBlob>(
                            new BlobAttribute($"transmission-incoming/{batchId}", FileAccess.ReadWrite));

                        await blobWriter.UploadTextAsync(String.Join(",", messages));
                    }
                    catch (Exception exIncomingBlob)
                    {
                        log.LogError($"Failed to log the incoming transmission blob: {batchId}. {exIncomingBlob.Message}");
                        throw exIncomingBlob;
                    }
                }

            }
            catch (Exception)
            {
                throw;
            }

            log.LogInformation($"[5064] : decomposed count {decomposed.Count}");
            if (decomposed.Count > 0)
            {
                log.LogInformation("[5064] : Before sending mesasge to splunk");
                var splunkMsgs = (SplunkEventMessages)Activator.CreateInstance(typeof(T2), outputEvents, log);
                try
                {
                    log.LogInformation("[5064] : Before Ingest mesasge to splunk");
                    splunkMsgs.Ingest(decomposed.ToArray());
                    log.LogInformation("[5064] : Before Emit mesasge to splunk");
                    await splunkMsgs.Emit();
                    log.LogInformation("[5064] : After Emit mesasge to splunk");
                }
                catch (Exception exEmit)
                {
                    log.LogInformation($"[5064] : After Emit mesasge to splunk Exception {exEmit.Message}\n{exEmit.StackTrace}");

                    try
                    {
                        var blobWriter = await blobFaultBinder.BindAsync<CloudBlockBlob>(
                            new BlobAttribute($"transmission-faults/{batchId}", FileAccess.ReadWrite));

                        string json = await Task<string>.Factory.StartNew(() => JsonConvert.SerializeObject(splunkMsgs.splunkEventMessages));
                        await blobWriter.UploadTextAsync(json);
                    }
                    catch (Exception exFaultBlob)
                    {
                        log.LogError($"Failed to write the fault blob: {batchId}. {exFaultBlob.Message}");
                        throw exFaultBlob;
                    }

                    try
                    {
                        var qMsg = new TransmissionFaultMessage { id = batchId, type = typeof(T2).ToString() };
                        string qMsgJson = JsonConvert.SerializeObject(qMsg);

                        var queueWriter = await queueFaultBinder.BindAsync<CloudQueue>(
                            new QueueAttribute("transmission-faults"));
                        await queueWriter.AddMessageAsync(new CloudQueueMessage(qMsgJson));
                    }
                    catch (Exception exFaultQueue)
                    {
                        log.LogError($"Failed to write the fault queue: {batchId}. {exFaultQueue.Message}");
                        throw exFaultQueue;
                    }

                    log.LogError($"Error emitting messages to output binding: {exEmit.Message}. The messages were held in the fault processor queue for handling once the error is resolved.");
                    throw exEmit;
                }
            }

            log.LogInformation($"C# Event Hub trigger function processed a batch of messages: {messages.Length}");
        }
    }
}
