#include "postgres.h"

#include <float.h>

#include "access/genam.h"
#include "access/table.h"
#include "access/tupdesc.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/itemptr.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "vector.h"
#include "access/heapam.h"  /* for heap_getnext / heap_getattr */
#include "access/relscan.h"
#include "vector_recall.h"

/* GUC variables */
bool		pgvector_track_recall = false;
int			pgvector_recall_sample_rate = 100;	/* Sample every 100th query */
int			pgvector_recall_max_samples = 10000;

typedef struct VectorRecallStats
{
	int64		total_queries;
	int64		sampled_queries;
	int64		total_results_returned;
	int64		correct_matches;
	int64		total_expected;
	double		current_recall;
	double		avg_results_per_query;
	TimestampTz	last_updated;
} VectorRecallStats;

void TrackVectorQuery(Relation index, Datum query_vector, int limit, double kth_distance, ItemPointerData *results, int num_results, FmgrInfo *distance_proc, Oid collation);
double GetCurrentRecall(Oid indexoid);
void ResetRecallStats(Oid indexoid);

static HTAB *recall_stats_hash = NULL;
static MemoryContext recall_context = NULL;

typedef struct RecallStatsEntry
{
	Oid			indexoid;
	VectorRecallStats stats;
} RecallStatsEntry;

void
InitVectorRecallTracking(void)
{
	HASHCTL		info;

	if (recall_context == NULL)
	{
		recall_context = AllocSetContextCreate(TopMemoryContext,
											   "Vector Recall Tracking",
											   ALLOCSET_DEFAULT_SIZES);
	}

	if (recall_stats_hash == NULL)
	{
		MemSet(&info, 0, sizeof(info));
		info.keysize = sizeof(Oid);
		info.entrysize = sizeof(RecallStatsEntry);
		info.hcxt = recall_context;

		recall_stats_hash = hash_create("Vector Recall Stats",
										32,
										&info,
										HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	/* Define GUC parameters */
	DefineCustomBoolVariable("pgvector.track_recall",
							 "Enables recall tracking for vector queries",
							 "When enabled, pgvector will sample queries to measure recall quality.",
							 &pgvector_track_recall,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("pgvector.recall_sample_rate",
							"Sets the sampling rate for recall tracking (1 in N queries)",
							"Higher values mean less frequent sampling, lower overhead.",
							&pgvector_recall_sample_rate,
							100, 1, 10000,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pgvector.recall_max_samples",
							"Maximum number of recall samples to maintain per index",
							"Older samples are discarded when this limit is reached.",
							&pgvector_recall_max_samples,
							10000, 100, 1000000,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);
}

void
VectorRecallTrackerInit(VectorRecallTracker *tracker)
{
	tracker->query_value = (Datum) 0;
	tracker->result_count = 0;
	tracker->results = NULL;
	tracker->results_capacity = 0;
	tracker->max_distance = 0.0;
}

void
VectorRecallTrackerDefaults(VectorRecallTracker *tracker)
{
	tracker->results_capacity = 100; /* Initial capacity */
	tracker->results = palloc(tracker->results_capacity * sizeof(ItemPointerData));
}

void
VectorRecallUpdate(VectorRecallTracker *tracker, ItemPointerData *heaptid)
{
	if (tracker->results == NULL)
		return;

	/* Expand array if needed */
	if (tracker->result_count >= tracker->results_capacity) {
		tracker->results_capacity *= 2;
		tracker->results = repalloc(tracker->results, tracker->results_capacity * sizeof(ItemPointerData));
	}

	tracker->results[tracker->result_count] = *heaptid;
	tracker->result_count++;
}

void
VectorRecallUpdateDistance(VectorRecallTracker *tracker, double distance)
{
	if (distance > tracker->max_distance)
		tracker->max_distance = distance;
}

void
VectorRecallCleanup(VectorRecallTracker *tracker)
{
	if (tracker->results != NULL)
		pfree(tracker->results);
}

/*
 * Track a vector query with safe recall estimation
 */
void
TrackVectorQuery(
	Relation index,
	Datum query_vector,
	int limit,
	double kth_distance,
  ItemPointerData *results,
	int num_results,
	FmgrInfo *distance_proc,
  Oid collation
)
{
	RecallStatsEntry *entry;
	bool		found;
	static int	query_counter = 0;

	/* Don't proceed if tracking is disabled. */
	if (!pgvector_track_recall || recall_stats_hash == NULL)
		return;

	entry = (RecallStatsEntry *) hash_search(recall_stats_hash,
											  &index->rd_id,
											  HASH_ENTER,
											  &found);

	if (!found)
	{
		MemSet(&entry->stats, 0, sizeof(VectorRecallStats));
		entry->stats.last_updated = GetCurrentTransactionStartTimestamp();
	}

	entry->stats.total_queries++;
	entry->stats.total_results_returned += num_results;

	query_counter++;

	if (query_counter % pgvector_recall_sample_rate == 0)
	{
		int estimated_expected = limit;
		int estimated_correct = num_results;

		entry->stats.sampled_queries++;

		/* Enhanced estimation using distance-threshold scan */
		if (kth_distance > 0)
		{
			/* Identify heap relation and attribute number */
			Oid heapOid = index->rd_index->indrelid;
			int16 attnum = index->rd_index->indkey.values[0];
			Relation heapRel = table_open(heapOid, AccessShareLock);
			TableScanDesc heapScan = table_beginscan(heapRel, GetActiveSnapshot(), 0, NULL);
			HeapTuple tuple;
			int count_within = 0;
			bool exceeded = false;
			Datum distanceDatum;
			bool isnull;
			double dist;
			TupleDesc tupDesc = RelationGetDescr(heapRel);

			while ((tuple = heap_getnext(heapScan, ForwardScanDirection)) != NULL)
			{
				Datum value = heap_getattr(tuple, attnum, tupDesc, &isnull);
				if (isnull)
					continue;

				distanceDatum = FunctionCall2Coll(distance_proc, collation, query_vector, value);
				dist = DatumGetFloat8(distanceDatum);
				if (dist <= kth_distance + DBL_EPSILON)
				{
					count_within++;
					if (count_within > limit)
					{
						exceeded = true;
						break;
					}
				}
			}

			table_endscan(heapScan);
			table_close(heapRel, AccessShareLock);

			if (exceeded)
				estimated_expected = limit + 1; /* conservative lower bound */
			else
				estimated_expected = count_within;
		}

		entry->stats.correct_matches += estimated_correct;
		entry->stats.total_expected += estimated_expected;

		if (entry->stats.total_queries > 0)
			entry->stats.avg_results_per_query = (double) entry->stats.total_results_returned / entry->stats.total_queries;

		if (entry->stats.total_expected > 0)
			entry->stats.current_recall = (double) entry->stats.correct_matches / entry->stats.total_expected;

		entry->stats.last_updated = GetCurrentTransactionStartTimestamp();
	}
}

double
GetCurrentRecall(Oid indexoid)
{
	RecallStatsEntry *entry;

	if (recall_stats_hash == NULL)
		return -1.0;

	entry = (RecallStatsEntry *) hash_search(recall_stats_hash,
											  &indexoid,
											  HASH_FIND,
											  NULL);

	if (entry == NULL || entry->stats.total_expected == 0)
		return -1.0;

	return entry->stats.current_recall;
}

void
ResetRecallStats(Oid indexoid)
{
	RecallStatsEntry *entry;

	if (recall_stats_hash == NULL)
		return;

	entry = (RecallStatsEntry *) hash_search(recall_stats_hash,
											  &indexoid,
											  HASH_FIND,
											  NULL);

	if (entry != NULL)
	{
		MemSet(&entry->stats, 0, sizeof(VectorRecallStats));
		entry->stats.last_updated = GetCurrentTransactionStartTimestamp();
	}
}

/*
 * SQL function to get recall statistics
 */
PG_FUNCTION_INFO_V1(pg_vector_recall_stats);
Datum
pg_vector_recall_stats(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	HASH_SEQ_STATUS status;
	RecallStatsEntry *entry;

	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	tupdesc = CreateTemplateTupleDesc(8);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "indexoid", OIDOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "total_queries", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "sampled_queries", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "total_results_returned", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5, "correct_matches", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 6, "total_expected", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 7, "current_recall", FLOAT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 8, "last_updated", TIMESTAMPTZOID, -1, 0);

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	if (recall_stats_hash == NULL)
		return (Datum) 0;

	hash_seq_init(&status, recall_stats_hash);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		Datum		values[8];
		bool		nulls[8];

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));

		values[0] = ObjectIdGetDatum(entry->indexoid);
		values[1] = Int64GetDatum(entry->stats.total_queries);
		values[2] = Int64GetDatum(entry->stats.sampled_queries);
		values[3] = Int64GetDatum(entry->stats.total_results_returned);
		values[4] = Int64GetDatum(entry->stats.correct_matches);
		values[5] = Int64GetDatum(entry->stats.total_expected);
		values[6] = Float8GetDatum(entry->stats.current_recall);
		values[7] = TimestampTzGetDatum(entry->stats.last_updated);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	return (Datum) 0;
}

/*
 * SQL function to reset recall statistics for a specific index
 */
PG_FUNCTION_INFO_V1(pg_vector_recall_reset);
Datum
pg_vector_recall_reset(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);

	ResetRecallStats(indexoid);

	PG_RETURN_VOID();
}

/*
 * SQL function to get current recall for a specific index
 */
PG_FUNCTION_INFO_V1(pg_vector_recall_get);
Datum
pg_vector_recall_get(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	double		recall;

	recall = GetCurrentRecall(indexoid);

	if (recall < 0)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(recall);
}
