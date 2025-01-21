/*
 * save.c
 *
 * The function is used to save data in original format, such as CSV, etc.
 *
 * Copyright(C) by Shenzhen Jupiter Fund Management Co. Ltd. 2025-
 */

int
save(uint8_t *s_data, size_t s_len, const char *dst_fn)
{
	FILE *dst_fp = NULL;
	int len = 0;
	size_t out_len = 0;

	if (dst_fn == NULL || s_data == NULL) {
		return -1;
	}

	dst_fp = fopen(dst_fn, "w");
	if (dst_fp == NULL) {
		return -2;
	}

	while (out_len < s_len) {
		len = fwrite(s_data + out_len, s_len - out_len, dst_fp);
		if (len < 0) {
			break;
		}

		out_len += len;
	}

	return 0;
}

