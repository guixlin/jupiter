/*
 * tick.h
 *
 * The file contains the definition and declaration of the tick data structure,
 * and the functions' prototype for fetching tick data from exchange.
 * 
 * Copyright(C) by Shenzhen Jupiter Fund Management Co. Ltd. 2025-
 */

#ifndef __TICK_H__
#define __TICK_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

typedef struct pv_data
{
	double price;
	double volume;
} pv_data_t;

typedef struct tick_data
{
	uint64_t timestamp;	/* timestamp of the tick data */

	void *basic_data;	/* basic data of the tick data, such as open price, etc. */
	pv_data_t last;		/* last price and volume */
	int level;		/* level of the tick data */
	union {
		struct {
			pv_data_t *bid;		/* points to the first bid price and volume pair */
			pv_data_t *ask;		/* points to the first ask price and volume pair */
		} normal;
		struct {
			pv_data_t bid;
			pv_data_t ask;
		} *ba_pair;	/* points to the first bid and ask price and volume pair */
	} all_pvs;
} tick_data_t;

#endif		/* __TICK_H__ */