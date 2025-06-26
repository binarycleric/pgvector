#ifndef VECTOR_RECALL_H
#define VECTOR_RECALL_H

#include "postgres.h"

typedef struct VectorRecallTracker
{
	Datum		        query_value;
	int			        result_count;     /* number of results returned */
	ItemPointerData *results;
	int			        results_capacity; /* capacity of the results array */
	double		      max_distance;     /* distance of the farthest (k-th) result */
} VectorRecallTracker;

void VectorRecallTrackerInit(VectorRecallTracker *tracker);
void VectorRecallTrackerDefaults(VectorRecallTracker *tracker);
void VectorRecallUpdate(VectorRecallTracker *tracker, ItemPointerData *heaptid);
void VectorRecallUpdateDistance(VectorRecallTracker *tracker, double distance);
void VectorRecallCleanup(VectorRecallTracker *tracker);
#endif