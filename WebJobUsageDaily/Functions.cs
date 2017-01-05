using Commons;
using Microsoft.Azure.WebJobs;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;

namespace WebJobUsageDailyTimer
{
    public class Functions
    {
        // Run job once an hour to fetch last 6 hrs (?) billing logs
        public static void TimerJob([TimerTrigger("01:00:00")] TimerInfo timer, TextWriter log)
        {
            log.WriteLine("*************************************************************************");
            log.WriteLine("Functions:TimerJob starting. DateTimeUTC: {0}", DateTime.UtcNow);

            updateSubscriptions(log, DateTime.Now.AddHours(-2), DateTime.Now);
        }

        private static void updateSubscriptions(TextWriter log, DateTime sdt, DateTime edt)
        {
            List<Subscription> abis = Commons.Utils.GetSubscriptions();

            foreach (Subscription s in abis)
            {
                try
                {
                    BillingRequest      br                 = new BillingRequest(s.Id, s.OrganizationId, sdt, edt);
                    CloudStorageAccount storageAccount     = CloudStorageAccount.Parse(ConfigurationManager.ConnectionStrings["AzureWebJobsStorage"].ToString());
                    CloudQueueClient    queueClient        = storageAccount.CreateCloudQueueClient();
                    CloudQueue          subscriptionsQueue = queueClient.GetQueueReference(ConfigurationManager.AppSettings["ida:QueueBillingDataRequests"].ToString());

                    subscriptionsQueue.CreateIfNotExists();

                    var queueMessage = new CloudQueueMessage(JsonConvert.SerializeObject(br));

                    subscriptionsQueue.AddMessageAsync(queueMessage);

                    log.WriteLine(String.Format("Sent id for daily billing log: {0}", s.Id));

                    Commons.Utils.UpdateSubscriptionStatus(s.Id, DataGenStatus.Pending, DateTime.UtcNow);
                }
                catch (Exception e)
                {
                    log.WriteLine("WebJobUsageDaily - SendQueue: " + e.Message);
                }
            }
        }
    }
}
