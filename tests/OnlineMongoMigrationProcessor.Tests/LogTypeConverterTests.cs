using System.Text.Json;
using Xunit;

namespace OnlineMongoMigrationProcessor.Tests
{
    public class LogTypeConverterTests
    {
        [Fact]
        public void LogTypeConverter_ShouldConvertOldMessageStringToInfo()
        {
            // Arrange - simulate old log file with "Message" string instead of "Info"
            string oldJson = "{\"Message\":\"Test message\",\"Type\":\"Message\",\"Datetime\":\"2025-10-15T10:00:00Z\"}";
            
            // Act
            var logObject = JsonSerializer.Deserialize<LogObject>(oldJson);
            
            // Assert
            Assert.NotNull(logObject);
            Assert.Equal(LogType.Info, logObject.Type);  // Message automatically converted to Info
            Assert.Equal("Test message", logObject.Message);
        }

        [Fact]
        public void LogTypeConverter_ShouldConvertOldMessageNumericToInfo()
        {
            // Arrange - simulate old log file with numeric value 1 (Message)
            string oldJson = "{\"Message\":\"Test message\",\"Type\":1,\"Datetime\":\"2025-10-15T10:00:00Z\"}";
            
            // Act
            var logObject = JsonSerializer.Deserialize<LogObject>(oldJson);
            
            // Assert
            Assert.NotNull(logObject);
            Assert.Equal(LogType.Info, logObject.Type);  // Numeric 1 (Message) converted to Info
            Assert.Equal("Test message", logObject.Message);
        }

        [Fact]
        public void LogTypeConverter_ShouldHandleNewInfoValue()
        {
            // Arrange - new log file with "Info"
            string newJson = "{\"Message\":\"Test message\",\"Type\":\"Info\",\"Datetime\":\"2025-10-15T10:00:00Z\"}";
            
            // Act
            var logObject = JsonSerializer.Deserialize<LogObject>(newJson);
            
            // Assert
            Assert.NotNull(logObject);
            Assert.Equal(LogType.Info, logObject.Type);
        }

        [Fact]
        public void LogTypeConverter_ShouldHandleWarningValue()
        {
            // Arrange
            string json = "{\"Message\":\"Test warning\",\"Type\":\"Warning\",\"Datetime\":\"2025-10-15T10:00:00Z\"}";
            
            // Act
            var logObject = JsonSerializer.Deserialize<LogObject>(json);
            
            // Assert
            Assert.NotNull(logObject);
            Assert.Equal(LogType.Warning, logObject.Type);
        }

        [Fact]
        public void LogTypeConverter_ShouldHandleErrorValue()
        {
            // Arrange
            string json = "{\"Message\":\"Test error\",\"Type\":\"Error\",\"Datetime\":\"2025-10-15T10:00:00Z\"}";
            
            // Act
            var logObject = JsonSerializer.Deserialize<LogObject>(json);
            
            // Assert
            Assert.NotNull(logObject);
            Assert.Equal(LogType.Error, logObject.Type);
        }

        [Fact]
        public void LogTypeConverter_ShouldHandleDebugValue()
        {
            // Arrange
            string json = "{\"Message\":\"Test debug\",\"Type\":\"Debug\",\"Datetime\":\"2025-10-15T10:00:00Z\"}";
            
            // Act
            var logObject = JsonSerializer.Deserialize<LogObject>(json);
            
            // Assert
            Assert.NotNull(logObject);
            Assert.Equal(LogType.Debug, logObject.Type);
        }

        [Fact]
        public void LogTypeConverter_ShouldHandleVerboseValue()
        {
            // Arrange
            string json = "{\"Message\":\"Test verbose\",\"Type\":\"Verbose\",\"Datetime\":\"2025-10-15T10:00:00Z\"}";
            
            // Act
            var logObject = JsonSerializer.Deserialize<LogObject>(json);
            
            // Assert
            Assert.NotNull(logObject);
            Assert.Equal(LogType.Debug, logObject.Type);
        }

        [Fact]
        public void LogTypeConverter_ShouldSerializeInfoAsString()
        {
            // Arrange
            var logObject = new LogObject(LogType.Info, "Test message");
            
            // Act
            string json = JsonSerializer.Serialize(logObject);
            
            // Assert - should write as "Info" not "Message"
            Assert.Contains("\"Type\":\"Info\"", json);
        }

        [Fact]
        public void LogTypeConverter_ShouldSerializeDeprecatedMessageAsInfo()
        {
            // Arrange - if someone uses the deprecated Message enum
            #pragma warning disable CS0618 // Type or member is obsolete
            var logObject = new LogObject(LogType.Message, "Test message");
            #pragma warning restore CS0618
            
            // Act
            string json = JsonSerializer.Serialize(logObject);
            
            // Assert - should write as "Info" not "Message"
            Assert.Contains("\"Type\":\"Info\"", json);
        }
    }
}
