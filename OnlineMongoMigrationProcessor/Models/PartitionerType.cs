namespace OnlineMongoMigrationProcessor
{
    public enum PartitionerType
    {
        UseSampleCommand,
        UseTimeBoundaries,
        UseAdjustedTimeBoundaries,
        UsePagination
    }
}
