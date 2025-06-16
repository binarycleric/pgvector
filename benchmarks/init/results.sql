SELECT
    function_name,
    test_name,
    vector_size,
    iterations,
    round((min_time * 1000000)::numeric, 3) as "Minimum Time (us)",
    round((max_time * 1000000)::numeric, 3) as "Maximum Time (us)",
    round((stddev_time * 1000000)::numeric, 3) as "Standard Deviation",
    round((p95_time * 1000000)::numeric, 3) as "95th Percentile (us)",
    round((p99_time * 1000000)::numeric, 3) as "99th Percentile (us)"
FROM benchmark_results
ORDER BY function_name, vector_size, test_name;
