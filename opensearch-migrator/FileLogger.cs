﻿using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace opensearch_migrator
{
    // FileLogger.cs
    public class FileLogger : ILogger
    {
        private readonly string _logFilePath;

        public FileLogger(string logFilePath = "migration_log.txt")
        {
            var dateTime = DateTime.UtcNow.ToString();
            DateTimeOffset dateTimeOffset = DateTimeOffset.Parse(dateTime, null, DateTimeStyles.RoundtripKind);
            long unixTimeMilliseconds = dateTimeOffset.ToUnixTimeMilliseconds();
            _logFilePath = $"{logFilePath}_{unixTimeMilliseconds}";

        }

        public void Log(string message)
        {
            string timestampedMessage = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}";
            Console.WriteLine(timestampedMessage);
            try
            {
                File.AppendAllText(_logFilePath, timestampedMessage + Environment.NewLine);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to write to log file: {ex.Message}");
            }
        }
    }
}
