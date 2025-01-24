/*
 * crawl_daily.h
 *
 * The file contains all global functions' prototype for crawl daily BAR data from exchange.
 *
 * Copyright(C) by Shenzhen Jupiter Fund Management Co. Ltd. 2025-
 */

#ifndef __CRAWL_DAILY_H__
#define __CRAWL_DAILY_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

int initialize(void);
void destroy(void);

size_t fetch_url(const char *url, uint8_t *data, size_t len);

int save(uint8_t *data, size_t size, const char *dst_fn);

#endif		/* __CRAWL_DAILY_H__ */

