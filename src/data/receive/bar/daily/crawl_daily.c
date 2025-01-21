/*
 * crawl_daily.c
 *
 * The main function is used to crawl the daily bar data from exchange.
 * 
 * Copyright(C) by Jupiter Fund 2025-
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <curl/curl.h>

int
main(int argc, char *argv[])
{
	if (argc != 2) {
		fprintf(stderr, "Usage: %s <URL>\n", argv[0]);
		return 1;
	}

	/* allocate 8MB data. */
	uint8_t *data = (uint8_t *)malloc(8 * 1024 * 1024);
	if (data == NULL) {
		fprintf(stderr, "malloc() failed\n");
		return 1;
	}

	if (initialize() != 0) {
		fprintf(stderr, "initialize() failed\n");
		return 1;
	}

	if (fetch_url(argv[1], data, 1024 * 1024) != 0) {
		fprintf(stderr, "fetch_url() failed\n");
		return 1;
	}

	destroy();

	return 0;
}
