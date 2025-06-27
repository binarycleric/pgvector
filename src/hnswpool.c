#include "postgres.h"

#include "hnsw.h"
#include "utils/memutils.h"
#include "miscadmin.h"

/* Global memory pool for HNSW tuple allocations */
HnswMemoryPool *hnsw_tuple_pool = NULL;

/*
 * Create a new memory pool
 */
HnswMemoryPool *
HnswCreatePool(Size chunk_size, int initial_chunks, MemoryContext parent_context)
{
	HnswMemoryPool *pool;
	MemoryContext old_context;
	MemoryContext pool_context;
	Size		total_memory;
	Size		freelist_size;
	int			i;

	/* Create dedicated memory context for this pool */
	pool_context = AllocSetContextCreate(parent_context,
										 "HNSW Memory Pool",
										 ALLOCSET_DEFAULT_SIZES);

	old_context = MemoryContextSwitchTo(pool_context);

	/* Allocate pool structure */
	pool = (HnswMemoryPool *) palloc0(sizeof(HnswMemoryPool));
	pool->pool_context = pool_context;
	pool->chunk_size = MAXALIGN(chunk_size);  /* Ensure proper alignment */
	pool->total_chunks = initial_chunks;
	pool->freelist_count = initial_chunks;
	pool->enabled = true;

	/* Allocate main memory block */
	total_memory = pool->chunk_size * initial_chunks;

	/* Check for integer overflow */
	if (initial_chunks > 0 && total_memory / initial_chunks != pool->chunk_size)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("memory pool size too large: %zu chunks * %zu bytes would overflow",
						(size_t) initial_chunks, pool->chunk_size)));
	}

	pool->pool_memory = (char *) palloc(total_memory);

	/* Allocate freelist array */
	freelist_size = sizeof(void *) * initial_chunks;
	pool->freelist = (void **) palloc(freelist_size);

	/* Initialize freelist with all chunks */
	for (i = 0; i < initial_chunks; i++)
	{
		pool->freelist[i] = pool->pool_memory + (i * pool->chunk_size);
	}

	MemoryContextSwitchTo(old_context);

	ereport(DEBUG1, (errmsg("HNSW memory pool created: %d chunks of %zu bytes (%zu MB total)",
							initial_chunks, pool->chunk_size, total_memory / (1024 * 1024))));

	return pool;
}

/*
 * Destroy a memory pool
 */
void
HnswDestroyPool(HnswMemoryPool *pool)
{
	if (pool == NULL)
		return;

	ereport(DEBUG1, (errmsg("HNSW memory pool destroyed: %d/%d chunks were used",
							pool->total_chunks - pool->freelist_count, pool->total_chunks)));

	MemoryContextDelete(pool->pool_context);
}

/*
 * Allocate from memory pool
 */
void *
HnswPoolAlloc(HnswMemoryPool *pool, Size size)
{
	void	   *result;

	/* Check if pool is available and size fits */
	if (pool == NULL || !pool->enabled || size > pool->chunk_size || pool->freelist_count == 0)
	{
		/* Log pool exhaustion for monitoring */
		if (pool != NULL && pool->enabled && pool->freelist_count == 0)
		{
			ereport(DEBUG2, (errmsg("HNSW memory pool exhausted, falling back to palloc")));
		}

		/* Fall back to regular allocation */
		return palloc0(size);
	}

	/* Get chunk from freelist */
	pool->freelist_count--;
	result = pool->freelist[pool->freelist_count];

	/* Zero only the requested memory, not the entire chunk */
	MemSet(result, 0, size);

	return result;
}

/*
 * Free memory back to pool
 */
void
HnswPoolFree(HnswMemoryPool *pool, void *ptr)
{
	char	   *char_ptr = (char *) ptr;



	/* Check if this pointer belongs to our pool */
	if (pool == NULL || !pool->enabled || ptr == NULL)
	{
		/* Fall back to regular free */
		if (ptr != NULL)
			pfree(ptr);
		return;
	}

	/* Check if pointer is within pool bounds */
	if (char_ptr < pool->pool_memory ||
		char_ptr >= pool->pool_memory + (pool->chunk_size * pool->total_chunks))
	{
		/* Not from our pool, use regular free */
		pfree(ptr);
		return;
	}

	/* Check if we have space in freelist */
	if (pool->freelist_count >= pool->total_chunks)
	{
		/* Pool freelist is full, this shouldn't happen but handle gracefully */
		ereport(WARNING, (errmsg("HNSW memory pool freelist overflow")));
		return;
	}

	/* Check for double-free by scanning freelist */
	for (int i = 0; i < pool->freelist_count; i++)
	{
		if (pool->freelist[i] == ptr)
		{
			ereport(WARNING, (errmsg("HNSW memory pool double-free detected")));
			return;
		}
	}

	/* Return chunk to freelist */
	pool->freelist[pool->freelist_count] = ptr;
	pool->freelist_count++;
}

/*
 * Initialize global memory pools
 */
void
HnswInitMemoryPools(void)
{
	/* Only initialize once */
	if (hnsw_tuple_pool != NULL)
		return;



	/* Create pool for HNSW tuple allocations */
	hnsw_tuple_pool = HnswCreatePool(HNSW_TUPLE_ALLOC_SIZE,
									 HNSW_POOL_INITIAL_CHUNKS,
									 TopMemoryContext);

	ereport(DEBUG1, (errmsg("HNSW memory pools initialized")));
}

/*
 * Cleanup global memory pools
 */
void
HnswCleanupMemoryPools(void)
{
	if (hnsw_tuple_pool != NULL)
	{
		HnswDestroyPool(hnsw_tuple_pool);
		hnsw_tuple_pool = NULL;
		ereport(DEBUG1, (errmsg("HNSW memory pools cleaned up")));
	}
}
