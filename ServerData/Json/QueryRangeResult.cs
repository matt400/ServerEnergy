namespace ServerData.Json;

public class QueryRangeResult
{
    public QueryRangeMetric? Metric { get; set; }
    public List<List<object>>? Values { get; set; }
}