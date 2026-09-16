/**
 * @brief  micros and structures for a simple PNG file 
 *
 * Copyright 2018-2020 Yiqing Huang
 * Updated 2024 m27ma
 *
 * This software may be freely redistributed under the terms of MIT License
 */
#pragma once
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <curl/curl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <arpa/inet.h>
#include <stdint.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/shm.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <curl/curl.h>
#include <libxml/HTMLparser.h>
#include <libxml/parser.h>
#include <libxml/xpath.h>
#include <libxml/uri.h>


#define SEED_URL "http://ece252-1.uwaterloo.ca:2530/image?img=1&part=1"
#define ECE252_HEADER "X-Ece252-Fragment: "
#define BUF_SIZE 1048576  /* 1024*1024 = 1M */
#define BUF_INC  524288   /* 1024*512  = 0.5M */

#define CT_PNG  "image/png"
#define CT_HTML "text/html"
#define CT_PNG_LEN  9
#define CT_HTML_LEN 9

#define PNG_SIG_SIZE    8 /* number of bytes of png image signature data */
#define CHUNK_LEN_SIZE  4 /* chunk length field size in bytes */          
#define CHUNK_TYPE_SIZE 4 /* chunk type field size in bytes */
#define CHUNK_CRC_SIZE  4 /* chunk CRC field size in bytes */
#define DATA_IHDR_SIZE 13 /* IHDR chunk data field size */
// #define BUF_SIZE 10000
#define URL_LEN 45 //not including the null terminator
#define ECE252_HEADER "X-Ece252-Fragment: "
#define BUF_INC  524288   /* 1024*512  = 0.5M */
#define NUM_MACHINES 3
#define NUM_IMAGE_SEGMENTS 50

#define max(a, b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a > _b ? _a : _b; })
     

typedef unsigned char U8;
typedef unsigned int  U32;
typedef unsigned long int U64;


typedef struct decoded_IDAT_chunk {
    U64 length;  /* length of data in the chunk, host byte order */
    U8  p_data[10000]; /* pointer to location where the actual data are */
} decoded_IDAT_chunk, *decoded_IDAT_chunk_p;

typedef struct chunk {
    U32 length;  /* length of data in the chunk, host byte order */
    U8  type[4]; /* chunk type */
    U8  *p_data; /* pointer to location where the actual data are */
    U32 crc;     /* CRC field  */
} *chunk_p;

typedef struct data_IHDR {// IHDR chunk data field
    U32 width;        /* width in pixels, big endian   */
    U32 height;       /* height in pixels, big endian  */
    U8  bit_depth;    /* num of bits per sample or per palette index.
                         valid values are: 1, 2, 4, 8, 16 */
    U8  color_type;   /* =0: Grayscale; =2: Truecolor; =3 Indexed-color
                         =4: Greyscale with alpha; =6: Truecolor with alpha */
    U8  compression;  /* only method 0 is defined for now */
    U8  filter;       /* only method 0 is defined for now */
    U8  interlace;    /* =0: no interlace; =1: Adam7 interlace */
} *data_IHDR_p;

typedef struct recv_buf2 {
    char *buf;       /* memory to hold a copy of received data */
    size_t size;     /* size of valid data in buf in bytes*/
    size_t max_size; /* max capacity of buf in bytes*/
    int seq;         /* >=0 sequence number extracted from http header */
                     /* <0 indicates an invalid seq number */
} RECV_BUF;

typedef struct thread_arg {
     RECV_BUF **p_results;
     char* url;
} THREAD_ARG;

typedef struct thread_ret_decoded_IDAT_chunk {
    U64 length;  /* length of data in the chunk, host byte order */
    U8* p_data; /* pointer to location where the actual data are */
} thread_ret_decoded_IDAT_chunk, *thread_ret_decoded_IDAT_chunk_p;  

typedef struct {
    U32 consumer_index;
    U32 producer_index;
    sem_t filled;
    sem_t empty;
    // RECV_BUF **inflated_data;
    atomic_bool done_consuming;
    atomic_uint inflated_data_size;
    atomic_uint concat_png_final_encoded_size;
    atomic_uint concat_png_decoded_bytes_size;
    atomic_uint num_consumed;
    atomic_bool track_consumed[50];
    atomic_bool track_produced[50];
} shared_data;

/* you're free to design and declare your own functions prototypes here*/
void *get_image_slice(void *arg); // thread function
size_t header_cb_curl(char *p_recv, size_t size, size_t nmemb, void *userdata);
size_t write_cb_curl3(char *p_recv, size_t size, size_t nmemb, void *p_userdata);
int recv_buf_init(RECV_BUF *ptr, size_t max_size);
int recv_buf_cleanup(RECV_BUF *ptr);
int write_file(const char *path, const void *in, size_t len);
int get_idat_data_chunk(RECV_BUF *curr_png_slice, decoded_IDAT_chunk_p decoded_buf, shared_data *shared_mem);
void catPNG(RECV_BUF *p_results[] , U32 size);
htmlDocPtr mem_getdoc(char *buf, int size, const char *url);
xmlXPathObjectPtr getnodeset (xmlDocPtr doc, xmlChar *xpath);
int find_http(char *fname, int size, int follow_relative_links, const char *base_url);
size_t header_cb_curl(char *p_recv, size_t size, size_t nmemb, void *userdata);
size_t write_cb_curl3(char *p_recv, size_t size, size_t nmemb, void *p_userdata);
int recv_buf_init(RECV_BUF *ptr, size_t max_size);
int recv_buf_cleanup(RECV_BUF *ptr);
void cleanup(CURL *curl, RECV_BUF *ptr);
int write_file(const char *path, const void *in, size_t len);
CURL *easy_handle_init(RECV_BUF *ptr);
int process_data(CURL *curl_handle, RECV_BUF *p_recv_buf);
long get_response_code(CURL *curl_handle, RECV_BUF *p_recv_buf);
char* get_content_type(CURL *curl_handle, RECV_BUF *p_recv_buf);
