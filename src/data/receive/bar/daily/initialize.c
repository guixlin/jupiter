/*
 * initialize.c
 *
 * The function is used to initialize the environment with libcurl to crawl the daily bar data from exchange.
 * The function is called by the main function.
 * 
 * Copyright(C) by Jupiter Fund 2025-
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <curl/curl.h>

int
initialize(void)
{
	curl_global_init(CURL_GLOBAL_ALL);

	return 0;
}

