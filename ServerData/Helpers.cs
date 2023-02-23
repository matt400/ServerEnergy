namespace ServerData;

internal static class Helpers
{
    public static DateOnly GetDateFromUnix(long unix) =>
        DateOnly.FromDateTime(DateTimeOffset.FromUnixTimeSeconds(unix).LocalDateTime);
}

