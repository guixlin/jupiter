/*
 * bar.h
 * 
 * The header file contains the definition and declaration of the bar data.
 * 
 * Copyright(C) by Jupiter Fund 2025-
 */

#ifndef _BAR_H_
#define _BAR_H_

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum bar_type {
	BAR_T_MIN = 0,
	BAR_T_5MIN,
	BAR_T_15MIN,
	BAR_T_30MIN,
	BAR_T_HOUR,
	BAR_T_DAY,
	BAR_T_WEEK,
	BAR_T_MONTH,
	BAR_T_YEAR,
	BAR_T_MAX
};

typedef struct bar_data
{
	uint64_t timestamp;	/* timestamp of the bar data */

	int type;		/* type of the bar data */
	char symbol[32];	/* symbol of the bar data */
	char exchange[32];	/* exchange of the bar data */

	double open;		/* open price */
	double high;		/* high price */
	double low;		/* low price */
	double close;		/* close price */
	double volume;		/* volume */
	double open_interest;	/* open interest */
	double amount;		/* amount */
}

#endif		/* _BAR_H_ */
