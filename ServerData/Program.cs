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
    private static readonly string listOfUptimeMetric = "increase((node_time_seconds - node_boot_time_seconds)[1d:1m])";

    private static readonly long initialDate = DateTime.Today.AddDays(-15).ToUnixTimestamp();
    private static readonly long dayBeforeDate = DateTime.Today.AddDays(-1).ToUnixTimestamp();

    private static readonly string? connectionString = Environment.GetEnvironmentVariable("POSTGRES_CONNECTION_STRING");

    private class RangeValues
    {
        public DateOnly Date { get; set; } 
        public JsonElement Val { get; set; }
    }

    private class EnergyHistory
    {
        public DateOnly Created { get; set; }
        public float Kwh { get; set; }
        public decimal Cost { get; set; }
        public int Downtime { get; set; }
        public int RateId { get; set; }
    }

    private class RateSet
    {
        public DateOnly Date { get; set; }
        public int RateId { get; set; }
        public float RateValue { get; set; }
    }

    static async Task Main(string[] args)
    {
        var apiEnergyData = await GetDataFromAPI(listOfkWhMetric);
        var apiUptimeData = await GetDataFromAPI(listOfUptimeMetric);

        await using var dataSource = NpgsqlDataSource.Create(connectionString ?? "");

        List<RangeValues>? apiEnergyConverted = apiEnergyData?.SelectMany(x =>
            new List<RangeValues>() {
                new RangeValues {
                    Date = Helpers.GetDateFromUnix(((JsonElement)x[0]).GetInt64()),
                    Val = (JsonElement)x[1]
                },
            }).ToList();

        List<DateOnly> dbExistingData = await GetExistingDates(dataSource, apiEnergyConverted);
        IEnumerable<RangeValues>? nonExistingData = apiEnergyConverted?.Where(x => !dbExistingData.Contains(x.Date));

        if (nonExistingData == null || !nonExistingData.Any())
        {
            Console.WriteLine("Wygląda na to, że dane z bazą są aktualne.");
            return;
        }

        Dictionary<DateOnly, double>? apiUptimeConverted = apiUptimeData?.SelectMany(x =>
            new Dictionary<DateOnly, double>() {
                [Helpers.GetDateFromUnix(((JsonElement)x[0]).GetInt64())] =
                    Math.Round(Convert.ToDouble(((JsonElement)x[1]).GetString()!.Replace('.', ',')), 0),
            }).ToDictionary(x => x.Key, y => y.Value);

        var rates = await GetCurrentRate(dataSource, apiUptimeConverted?.Keys.ToList());

        List<EnergyHistory> finalDataTable = new();

        foreach (var item in nonExistingData)
        {
            var getDowntime = apiUptimeConverted?[item.Date];

            RateSet? findRateByDate = rates.Find(x => x.Date == item.Date)
                ?? throw new Exception("Nie można znaleźć dopasowania daty do stawki.");

            finalDataTable.Add(new EnergyHistory()
            {
                Created = item.Date,
                Cost = Convert.ToDecimal(item.Val.GetString(), CultureInfo.InvariantCulture) * (decimal)findRateByDate.RateValue,
                Downtime = getDowntime != null ? 86400 - Convert.ToInt32(getDowntime, CultureInfo.InvariantCulture) : -1,
                Kwh = Convert.ToSingle(item.Val.GetString(), CultureInfo.InvariantCulture),
                RateId = findRateByDate.RateId 
            });
        }

        await SaveDataToDb(dataSource, finalDataTable);
    }

    private static async Task SaveDataToDb(NpgsqlDataSource dataSource, List<EnergyHistory> data)
    {
        await using var cmd = dataSource.CreateCommand(@"
            INSERT INTO ""EnergyHistory"" (""Created"", ""Kwh"", ""Cost"", ""Downtime"", ""EnergyRateId"") VALUES (@a, @b, @c, @d, @e)");

        var created = new NpgsqlParameter<DateOnly>("a", default(DateOnly));
        var kwh = new NpgsqlParameter<float>("b", 0.0f);
        var cost = new NpgsqlParameter<decimal>("c", 0.0M);
        var downtime = new NpgsqlParameter<int>("d", 0);
        var rateId = new NpgsqlParameter<int>("e", 0);

        cmd.Parameters.Add(created);
        cmd.Parameters.Add(kwh);
        cmd.Parameters.Add(cost);
        cmd.Parameters.Add(downtime);
        cmd.Parameters.Add(rateId);

        foreach (var item in data)
        {
            created.Value = item.Created;
            kwh.Value = item.Kwh;
            cost.Value = item.Cost;
            downtime.Value = item.Downtime;
            rateId.Value = item.RateId;

            await cmd.ExecuteNonQueryAsync();

            Console.WriteLine($"Dodano dane z dnia {item.Created} do bazy.");
        }
    }

    private static async Task<List<RateSet>> GetCurrentRate(NpgsqlDataSource dataSource, List<DateOnly>? dates)
    {
        if (dates == null)
        {
            Console.WriteLine("Nie można pobrać stawki dla określonego dnia.");
            throw new ArgumentNullException(nameof(dates));
        }

        string prepareDates = $"{{{ string.Join(',', dates.Select(x => x.ToPostgresDate())) }}}";

        await using var cmd = dataSource.CreateCommand($"select get_rate_by_date('{prepareDates}');");
        await using var reader = await cmd.ExecuteReaderAsync();

        List<RateSet> list = new();

        while (await reader.ReadAsync()) {
            object[] r = (object[])reader[0];

            list.Add(new RateSet() {
                Date = DateOnly.FromDateTime((DateTime)r[0]),
                RateId = (int)r[1],
                RateValue = (float)r[2]
            });
        }

        return list;
    }

    private static async Task<List<DateOnly>> GetExistingDates(NpgsqlDataSource dataSource, List<RangeValues>? listOfDates)
    {
        if (listOfDates == null)
            throw new ArgumentNullException(nameof(listOfDates));

        var sqlWhere = string.Join(',', listOfDates.Select(x => "'" + x.Date.ToPostgresDate() + "'"));

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
        $@"{psApi}{psQueryRange}{metric}&step=1d&start={initialDate}&end={dayBeforeDate}";
}

