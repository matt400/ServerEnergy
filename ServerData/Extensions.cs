using System.Globalization;

namespace ServerData;

public static class Extensions
{
    public static long ToUnixTimestamp(this DateTime value) =>
        (long)(value.ToUniversalTime().Subtract(new DateTime(1970, 1, 1))).TotalSeconds;

    public static string ToPostgresDate(this DateOnly value) =>
        value.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
}

