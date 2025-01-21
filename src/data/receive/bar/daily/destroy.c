/*
 * destroy.c
 *
 * The function is used to destroy the environment with libcurl to crawl the daily bar data from exchange.
 * 
 * Copyright(C) by Jupiter Fund 2025-
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <curl/curl.h>

void
destroy(void)
{
	curl_global_cleanup();
}
