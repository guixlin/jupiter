/*
 * fetch_url.c
 *
 * The function is used to fetch the specific URL with libcurl. It is called by the main function.
 * 
 * Copyright(C) by Jupiter Fund 2025-
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <curl/curl.h>

struct memory_chunk {
	uint8_t *data;
	size_t size;
};

static size_t 
write_data(void *ptr, size_t size, size_t nmemb, void *data_chunk)
{
	size_t total = size * nmemb;
	struct memory_chunk *chunk = (struct memory_chunk *)data_chunk;

	if (chunk->size < total) {
		fprintf(stderr, "data buffer is too small\n");

		chunk->size = 0;
		return 0;
	}

	memcpy(chunk->data, ptr, total);
	chunk->size = total;

	return total;
}

size_t
fetch_url(const char *url, uint8_t *data, size_t size)
{
	CURL *curl;
	CURLcode res;
	struct memory_chunk chunk;

	assert(url != NULL);
	assert(data != NULL);

	chunk.data = data;
	chunk.size = size;
	
	curl = curl_easy_init();
	if (curl) {
		curl_easy_setopt(curl, CURLOPT_URL, url);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &chunk);
		curl_easy_setopt(curl, CURLOPT_USERAGENT, "fetch_daily_bar/1.0");
		res = curl_easy_perform(curl);

		if (res != CURLE_OK) {
			fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
			return 0;
		}
	}
	
	return chunk.size;
}

