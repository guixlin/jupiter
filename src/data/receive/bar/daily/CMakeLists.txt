#
# CMakeLists.txt
#
# Copyright(C) by Shenzhen Jupiter Fund Management Co., Ltd.

cmake_minimum_required(VERSION 3.20)

project(crawl_daily 
        VERSION 0.1
	DESCRIPTION "crawl daily bar data from exchange by specifying URL"
	LANGUAGES C)

add_executable(crawl_daily crawl_daily.c fetch_url.c initialize.c destroy.c save.c)

