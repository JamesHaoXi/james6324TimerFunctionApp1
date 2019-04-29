using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using System.Data.SqlClient;
using Microsoft.Azure.Devices;
using System.Threading.Tasks;
using System.Text;
using System.Diagnostics;
using System.Collections.Generic;
//using Microsoft.Extensions.Logging;

namespace james6324TimerFunctionApp1
{
    public static class Function1
    {
        static string connectionString = "HostName=james6342iot.azure-devices.net;SharedAccessKeyName=service;SharedAccessKey=RnzkBco8VRay0qFzzsVy4GFmpQF6uTDHVJInUD7VlQ4=";
        static string TestCmdString = "TEST ON";
        static ServiceClient serviceClient;

        [FunctionName("Function1")]
        public static void Run([TimerTrigger("*/30 * * * * *")]TimerInfo myTimer, TraceWriter log)
        {
            //           connectionString = "HostName=james6342iot.azure-devices.net;SharedAccessKeyName=service;SharedAccessKey=RnzkBco8VRay0qFzzsVy4GFmpQF6uTDHVJInUD7VlQ4=";
            DateTime timeUtc = DateTime.UtcNow;
            DateTime cstTime = DateTime.Now;
            try
            {
                TimeZoneInfo cstZone = TimeZoneInfo.FindSystemTimeZoneById("AUS Eastern Standard Time");
                cstTime = TimeZoneInfo.ConvertTimeFromUtc(timeUtc, cstZone);
                Console.WriteLine("The date and time are {0} {1}.",
                                  cstTime,
                                  cstZone.IsDaylightSavingTime(cstTime) ?
                                          cstZone.DaylightName : cstZone.StandardName);
            }
            catch (TimeZoneNotFoundException)
            {
                Console.WriteLine("The registry does not define the Central Standard Time zone.");
            }
            catch (InvalidTimeZoneException)
            {
                Console.WriteLine("Registry data on the Central Standard Time zone has been corrupted.");
            }


            log.Info($"C# Timer trigger function executed at: {cstTime}");

            // Get the connection string from app settings and use it to create a connection.
            var str = Environment.GetEnvironmentVariable("sqldb_connection");
            //string tmp_str = "";
            IList<string> task_list = new List<string>();
            using (SqlConnection conn = new SqlConnection(str))
            {
                conn.Open();
                //var text = "UPDATE SalesLT.SalesOrderHeader " +
                //        "SET [Status] = 5  WHERE ShipDate < GetDate();";
                //var text = "SELECT * " +
                //        "FROM Job " +
                //        "WHERE Done = 0 AND deviceID='" + data.deviceID + "' " +
                //        "ORDER BY CreateTime DESC;";
                var text = "SELECT * " +
                        "FROM TestSchedule " +
                        "WHERE Done = 0;";
                using (SqlCommand cmd = new SqlCommand(text, conn))
                {
                    // Execute the command and log the # rows affected.

                    //var rows = cmd.ExecuteScalar();
                    //log.LogInformation($"{rows} rows were get from Job table.");
                    SqlDataReader reader = cmd.ExecuteReader();
                    //string tmp_str = "";
                    while (reader.Read())
                    {

                        TimeSpan span = DateTime.Parse(reader["ScheduleTime"].ToString()).Subtract(cstTime);
                        log.Info(reader["deviceID"].ToString() + "," + reader["ScheduleTime"].ToString() +" Td=" + span.TotalMinutes);
                       
                        //check if it's time to do test
                        if (span.TotalMinutes<=0)
                        {
                            if (!task_list.Contains(reader["deviceID"].ToString()))
                            {
                                task_list.Add(reader["deviceID"].ToString());

                            }

                            //
                        }

                    }
                    reader.Close();

                    foreach(string device in task_list)
                    {
                        // do the test
                        TestCmdString = "TEST ON";
                        log.Info("#### Send Cloud-to-Device message test on ######\n");
                        serviceClient = ServiceClient.CreateFromConnectionString(connectionString);
                        ReceiveFeedbackAsync();
                        SendCloudToDeviceMessageAsync().Wait();
                        log.Info("#### Send Cloud-to-Device message OK! ######\n");

                        //update mark the schedule task done
                        text = "UPDATE TestSchedule " +
                            "SET Done = 1 " +
                            "WHERE Done = 0 AND " + "deviceID = '" + device + "' " +
                            "AND ScheduleTime < '" + cstTime.ToString("yyyy-M-dd H:mm:ss") + "';";
              
                        using (SqlCommand cmd_done = new SqlCommand(text, conn))
                        {
                            var rows = cmd_done.ExecuteNonQuery();
                            log.Info($"{rows} rows were update from Job table." + cstTime.ToString("yyyy-M-dd H:mm:ss"));
                        }
                    }
                    //if (((int)rows <= 0) && (data.brightness < 185))
                    //{
                    //    text = "INSERT INTO Job (deviceID, CreateTime, Done)" +
                    //    "VALUES ( '" + data.deviceID + "','" + data.timestamp + "',0);";
                    //    SqlCommand cmd_insert = new SqlCommand(text, conn);

                    //    var rows_insert = cmd_insert.ExecuteNonQuery();
                    //    log.LogInformation($"{rows_insert} rows were inserted to Job table.");
                    //}


                }
                conn.Close();
            }

        }

        private async static Task SendCloudToDeviceMessageAsync()
        {
            var commandMessage = new
             Message(Encoding.ASCII.GetBytes(TestCmdString));
            commandMessage.Ack = DeliveryAcknowledgement.Full;
            await serviceClient.SendAsync("james6342IoTGateway", commandMessage);
        }

        private async static void ReceiveFeedbackAsync()
        {
            var feedbackReceiver = serviceClient.GetFeedbackReceiver();

            Debug.WriteLine("\n#####Receiving c2d feedback from service");
            while (true)
            {
                var feedbackBatch = await feedbackReceiver.ReceiveAsync();
                if (feedbackBatch == null) continue;

                Debug.WriteLine("#########Received feedback: {0}");


                await feedbackReceiver.CompleteAsync(feedbackBatch);
            }
        }
    }
}
