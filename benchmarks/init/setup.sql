CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE benchmark_results (
    test_name text,
    function_name text,
    vector_size integer,
    iterations integer,
    total_time float8,
    min_time float8,
    max_time float8,
    stddev_time float8,
    p95_time float8,
    p99_time float8
);

ALTER TABLE benchmark_results SET (autovacuum_enabled = false);
ALTER TABLE benchmark_results SET (toast.autovacuum_enabled = false);

-- Generate random floats similar to an OpenAI embedding
CREATE OR REPLACE FUNCTION generate_random_floats(size integer)
RETURNS float[] AS $$
BEGIN
    RETURN (
        SELECT array_agg(random() * 2 - 1)
        FROM generate_series(1, size)
    );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION generate_vector_series(size integer)
RETURNS vector(16) AS $$
BEGIN
    RETURN (
        SELECT array_agg((0.1 + (i * 0.01))::float4)::vector
        FROM generate_series(1, size) i
    );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION reverse_vector(v vector)
RETURNS vector
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT array_agg(val::float8 ORDER BY i DESC)::vector
  FROM unnest(v::float4[]) WITH ORDINALITY AS t(val, i);
$$;

CREATE TABLE small_vectors (
    id integer,
    embedding vector(16)
);

ALTER TABLE small_vectors SET (autovacuum_enabled = false);
ALTER TABLE small_vectors SET (toast.autovacuum_enabled = false);

INSERT INTO small_vectors (id, embedding)
SELECT i, generate_random_floats(16)
FROM generate_series(1, 100) i;

ANALYZE small_vectors;

CREATE OR REPLACE FUNCTION run_benchmark(
    test_name text,
    function_name text,
    vector_size integer,
    iterations integer,
    query text
) RETURNS void AS $$
DECLARE
    start_time float8;
    end_time float8;
    iteration_times float8[];
    i integer;
    sum_squares float8;
    mean_time float8;
BEGIN
    iteration_times := array_fill(0.0::float8, ARRAY[iterations]);

    -- Run iterations and collect times
    FOR i IN 1..iterations LOOP
        start_time := extract(epoch from clock_timestamp());
        EXECUTE query;
        end_time := extract(epoch from clock_timestamp());
        iteration_times[i] := end_time - start_time;
    END LOOP;

    -- Calculate total and average time
    end_time := extract(epoch from clock_timestamp());
    start_time := extract(epoch from clock_timestamp()) - (end_time - start_time);

    -- Sort times for percentile calculation
    SELECT array_agg(t ORDER BY t)
    INTO iteration_times
    FROM unnest(iteration_times) t;

    -- Calculate mean
    SELECT sum(t) / iterations
    INTO mean_time
    FROM unnest(iteration_times) t;

    -- Calculate sample standard deviation (using n-1 for unbiased estimate)
    SELECT sum(power(t - mean_time, 2))
    INTO sum_squares
    FROM unnest(iteration_times) t;

    INSERT INTO benchmark_results (
        test_name,
        function_name,
        vector_size,
        iterations,
        total_time,
        min_time,
        max_time,
        stddev_time,
        p95_time,
        p99_time
    ) VALUES (
        test_name,
        function_name,
        vector_size,
        iterations,
        end_time - start_time,
        iteration_times[1],  -- min is first element after sorting
        iteration_times[iterations],  -- max is last element after sorting
        sqrt(sum_squares / (iterations - 1)),  -- sample standard deviation
        iteration_times[floor(iterations * 0.95)],
        iteration_times[floor(iterations * 0.99)]
    );
END;
$$ LANGUAGE plpgsql;
