using System.Collections.Generic;

namespace OnlineMongoMigrationProcessor.Models
{
    public class StorageValidationResult
    {
        public long SampledDocumentCount { get; set; }
        public long EstimatedSourceDocumentCount { get; set; }
        public long UsedBytes { get; set; }
        public long DataBytes { get; set; }
        public long IndexBytes { get; set; }
        public double BytesPerDocument { get; set; }
        public double DataBytesPerDocument { get; set; }
        public double IndexBytesPerDocument { get; set; }
        public long EstimatedFullCollectionDataBytes { get; set; }
        public long EstimatedFullCollectionIndexBytes { get; set; }
        public long EstimatedFullCollectionBytes { get; set; }
        public long SourceDataBytes { get; set; }
        public long SourceIndexBytes { get; set; }
        public long SourceUsedBytes { get; set; }
        public double? DataGrowthPercent { get; set; }
        public double? IndexGrowthPercent { get; set; }
        public double? TotalGrowthPercent { get; set; }
        public List<StorageValidationShardResult> Shards { get; set; } = new();
        public List<StorageValidationShardResult> SourceShards { get; set; } = new();
    }

    public class StorageValidationShardResult
    {
        public string ShardName { get; set; } = string.Empty;
        public long SampledDocumentCount { get; set; }
        public long EstimatedSourceDocumentCount { get; set; }
        public long UsedBytes { get; set; }
        public long DataBytes { get; set; }
        public long IndexBytes { get; set; }
        public double BytesPerDocument { get; set; }
        public double DataBytesPerDocument { get; set; }
        public double IndexBytesPerDocument { get; set; }
        public long EstimatedFullCollectionBytes { get; set; }
    }
}