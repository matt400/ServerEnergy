using Npgsql;
using ServerData.Json;
using System.Data;
using System.Globalization;
using System.Net.Http.Json;
using System.Text.Json;

namespace ServerData;

internal class Program
{
    static readonly HttpClient client = new();

    private static readonly string psApi = "https://ps.server.m477.pl/api/v1";
    private static readonly string psQueryRange = "/query_range?query=";
    private static readonly string listOfkWhMetric = "max_over_time(tasmota_sensors_today_[1d])";
    private static readonly string listOfUptimeMetric = "node_time_seconds - node_boot_time_seconds";

    private static readonly long initialDate = DateTime.Today.AddDays(-15).ToUnixTimestamp();
    private static readonly long dayBeforeDate = DateTime.Today.AddDays(-1).ToUnixTimestamp();

    private static readonly string? connectionString = Environment.GetEnvironmentVariable("POSTGRES_CONNECTION_STRING");

    private class RangeValues
    {
        public DateOnly Date;
        public string? Val;
    }

    static async Task Main(string[] args)
    {
        var apiEnergyData = await GetDataFromAPI(listOfkWhMetric);
        var apiUptimeData = await GetDataFromAPI(listOfUptimeMetric);

        await using var dataSource = NpgsqlDataSource.Create(connectionString ?? "");

        List<RangeValues>? apiEnergyConverted = apiEnergyData?.SelectMany(x =>
            new List<RangeValues>() {
                new RangeValues {
                    Date = DateOnly.FromDateTime(DateTimeOffset.FromUnixTimeSeconds(((JsonElement)x[0]).GetInt64()).LocalDateTime),
                    Val = ((JsonElement)x[1]).ToString()
                },
            }).ToList();


        List<DateOnly> dbExistingData = await GetExistingDates(dataSource, apiEnergyConverted);
        IEnumerable<RangeValues>? notExistingData = apiEnergyConverted?.Where(x => !dbExistingData.Contains(x.Date));
    }

    private static async Task<List<DateOnly>> GetExistingDates(NpgsqlDataSource dataSource, List<RangeValues>? listOfDates)
    {
        if (listOfDates == null)
            throw new ArgumentNullException(nameof(listOfDates));

        var sqlWhere = string.Join(',', listOfDates.Select(x => "'" + x.Date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + "'"));

        await using var cmd = dataSource.CreateCommand(@$"
            select ""Created""
            from ""EnergyHistory""
            where ""Created""
            in ({sqlWhere})
            order by ""Created"" asc 
        ");

        await using var reader = await cmd.ExecuteReaderAsync();

        List<DateOnly> dates = new();

        while (await reader.ReadAsync())
            dates.Add(DateOnly.FromDateTime(reader.GetDateTime(0)));

        return dates;
    }

    private static async Task<List<List<object>>?> GetDataFromAPI(string metric)
    {
        List<List<object>>? data = new();

        try
        {
            HttpResponseMessage response = await client.GetAsync(PrometheusAPIUrl(metric));
            response.EnsureSuccessStatusCode();

            data = (await response.Content.ReadFromJsonAsync<QueryRange>())?.Data?.Result?.FirstOrDefault()?.Values;
        }
        catch(HttpRequestException e)
        {
            Console.WriteLine("\nException Caught!");	
            Console.WriteLine("Message :{0} ", e.Message);
        }

        return data;
    }

    private static string PrometheusAPIUrl(string metric) =>
        $"{psApi}{psQueryRange}{metric}&step=1d&start={initialDate}&end={dayBeforeDate}";
}

