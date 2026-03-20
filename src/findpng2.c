#include <search.h>
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
#include <time.h>
#include <libxml/HTMLparser.h>
#include <libxml/parser.h>
#include <libxml/xpath.h>
#include <libxml/uri.h>

#include "../include/lab_png.h"


#define MAX_NUM_ITEMS 2048

struct web_crawler_state {
    int num_threads;
    int target;
    int front_ptr;
    int back_ptr;
    int png_ptr;
    int url_ptr;
    int done;
    char** pngs_list; 
    char* queue[MAX_NUM_ITEMS];
    char* urls_list[MAX_NUM_ITEMS];
    pthread_mutex_t mutex;
    pthread_mutex_t url_lock;
    pthread_mutex_t png_lock;
    pthread_mutex_t q_lock;
    pthread_mutex_t parse_links_lock;
    pthread_mutex_t set_lock;
    pthread_cond_t cond;
    sem_t done_crawling;
    atomic_int threads_done_crawling;
    atomic_int idle_threads;
};

#define STREAM_BUFFER_ITEMS 10
struct web_crawler_state* crawl_state;

typedef struct {
    char **urls;           // Array of URL strings
    U8 **pngs;  // Array of PNG byte buffers
    size_t *sizes;         // Size of each PNG
    int back;              // Write position (producer)
    int front;              // Read position (consumer)
    int num_frames;             // How many items in buffer
    int num_frames_produced;
    int done;              // Flag: producer finished?
    pthread_mutex_t stream_buf_lock;
    sem_t filled;
    sem_t empty;
} stream_buf_state;

stream_buf_state* stream_state;

stream_buf_state* init_stream_buffer() {
    stream_buf_state *buf = malloc(sizeof(stream_buf_state));
    buf->urls = malloc(STREAM_BUFFER_ITEMS * sizeof(char*));
    buf->pngs = malloc(STREAM_BUFFER_ITEMS * sizeof(U8*));
    buf->sizes = malloc(STREAM_BUFFER_ITEMS * sizeof(size_t));
    buf->front = 0;
    buf->back = 0;
    buf->num_frames = 0;
    buf-> num_frames_produced = 0;
    buf->done = 0;
    
    pthread_mutex_init(&buf->stream_buf_lock, NULL);
    sem_init(&buf->filled, 0, 0);
    sem_init(&buf->empty, 0, STREAM_BUFFER_ITEMS);
    
    return buf;
}

static size_t stream_write_callback(void *contents, size_t size, size_t nmemb, void *userp) {
    size_t realsize = size * nmemb;
    FILE *pipe = (FILE *)userp;
    
    size_t written = fwrite(contents, size, nmemb, pipe);
    if (written != nmemb) {
        fprintf(stderr, "Error: Failed to write to FFmpeg pipe\n");
        return 0;
    }
    return realsize;
}

void* consumer_thread_func(void *arg) {
    stream_buf_state *buf = (stream_buf_state *)arg;
    // FILE *ffmpeg_pipe = popen(
    //     "ffmpeg -f image2pipe -pix_fmt rgb24 -s 1920x1080 -framerate 30 "
    //     "-i pipe:0 -c:v mpeg4 -q:v 5 output.mp4 2>/dev/null",
    //     "w"
    // );

FILE *ffmpeg_pipe = popen(
    "ffmpeg -y -f image2pipe "
    "-framerate 1 "          /* Interpret the incoming stream as 1 frame per second */
    "-i pipe:0 "             /* The input source */
    "-c:v mpeg4 "
    "-q:v 5 "
    "output.mp4",
    "w"
);

    setvbuf(ffmpeg_pipe, NULL, _IOFBF, 65536);
        
    // Keep consuming until producer signals done
    while (1) {
        // Write PNG bytes directly to FFmpeg
      // Initialize CURL
        CURL *curl = curl_easy_init();
        if (!curl) {
            fprintf(stderr, "Error: CURL init failed on consumer stream thread\n");
            continue;
        }
        sem_wait(&buf->filled);

        pthread_mutex_lock(&buf->stream_buf_lock);

        char *url = crawl_state->pngs_list[buf->front];
        printf("[Consumer] Got: %s \n", url);

        // Move tail pointer
        buf->front = (buf->front + 1) % STREAM_BUFFER_ITEMS;
        int frame_num = buf->num_frames++;
    
        // Signal producer that space is available
        sem_post(&buf->empty);

        if (buf->num_frames == buf->num_frames_produced && buf->done) {
            pthread_mutex_unlock(&buf->stream_buf_lock);
            break;
        }
    
        pthread_mutex_unlock(&buf->stream_buf_lock);
        
        curl_easy_setopt(curl, CURLOPT_URL, url);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, stream_write_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)ffmpeg_pipe);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

        CURLcode res = curl_easy_perform(curl);



        if (res == CURLE_OK) {
            fflush(ffmpeg_pipe);
            printf("[Consumer] Frame %d: Downloaded and piped %s\n", frame_num, url);
        } else {
            fprintf(stderr, "[Consumer] Error downloading %s: %s\n", url, curl_easy_strerror(res));
        }
        free(url);

        curl_easy_cleanup(curl);
    }
    
    pclose(ffmpeg_pipe);
    printf("[✓] Video complete: %d frames\n", buf->num_frames);
    
    return NULL;
}



int find_http_and_add(char *buf, int size, int follow_relative_links, const char *base_url) {
    int i;
    htmlDocPtr doc;
    xmlChar *xpath = (xmlChar*) "//a/@href";
    xmlNodeSetPtr nodeset;
    xmlXPathObjectPtr result;
    xmlChar *href;
        
    if (buf == NULL) {
        return -1;
    }

    doc = mem_getdoc(buf, size, base_url);
    if (doc == NULL)
        return -1;
    result = getnodeset (doc, xpath);
    if (result == NULL) {
        xmlFreeDoc(doc);
        return -1;
    }
    if (result) {
        nodeset = result->nodesetval;
        for (i=0; i < nodeset->nodeNr; i++) {
            href = xmlNodeListGetString(doc, nodeset->nodeTab[i]->xmlChildrenNode, 1);
            if ( follow_relative_links ) {
                xmlChar *old = href;
                href = xmlBuildURI(href, (xmlChar *) base_url);
                xmlFree(old);
            }
            if ( href != NULL && !strncmp((const char *)href, "http", 4) ) {
                char *href_copy = strdup((char*)href);
                xmlFree(href);

                ENTRY e, *ep;
                e.key = href_copy;
                e.data = NULL;
                pthread_mutex_lock(&crawl_state->set_lock);
                ep = hsearch(e, FIND);

                if (ep == NULL) {
                    hsearch(e, ENTER);

                    if (crawl_state->back_ptr >= MAX_NUM_ITEMS) {
                        pthread_mutex_unlock(&crawl_state->set_lock);
                        // xmlFree(href);
                        fprintf(stderr, "NEED TO ALLOCATE MORE SPACE IN THE BUFFERS, specifically in the queue - No longer adding new URLS to crawl!\n");
                        // exit(-1);
                        // pthread_mutex_lock(&crawl_state->png_lock);

                        // crawl_state->done=1;
                        // pthread_mutex_unlock(&crawl_state->png_lock);

                        return -2;
                    }
                    pthread_mutex_lock(&crawl_state->mutex);
                    crawl_state->queue[crawl_state->back_ptr++] = href_copy;
                    pthread_mutex_unlock(&crawl_state->mutex);

                    // printf("href: %s\n", href_copy);

                    // pthread_mutex_unlock(&crawl_state->q_lock);

                    pthread_cond_signal(&crawl_state->cond);
                } else {
                    free(href_copy);
                    href_copy = NULL;
                }

                pthread_mutex_unlock(&crawl_state->set_lock);

            } else {
                xmlFree(href);
            }
        }
        xmlXPathFreeObject (result);
    }
    xmlFreeDoc(doc);
    return 0;
}

void curl_set_options(CURL *curl_handle, RECV_BUF* ptr) {
    /* register write call back function to process received data */
    curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, write_cb_curl3); 
    /* user defined data structure passed to the call back function */
    curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *)ptr);

    /* register header call back function to process received header data */
    curl_easy_setopt(curl_handle, CURLOPT_HEADERFUNCTION, header_cb_curl); 
    /* user defined data structure passed to the call back function */
    curl_easy_setopt(curl_handle, CURLOPT_HEADERDATA, (void *)ptr);

    /* some servers requires a user-agent field */
    curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, "ece252 lab4 crawler");

    /* follow HTTP 3XX redirects */
    curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1L);
    /* continue to send authentication credentials when following locations */
    curl_easy_setopt(curl_handle, CURLOPT_UNRESTRICTED_AUTH, 1L);
    /* max numbre of redirects to follow sets to 5 */
    curl_easy_setopt(curl_handle, CURLOPT_MAXREDIRS, 5L);
    /* supports all built-in encodings */ 
    curl_easy_setopt(curl_handle, CURLOPT_ACCEPT_ENCODING, "");

    /* Max time in seconds that the connection phase to the server to take */
    //curl_easy_setopt(curl_handle, CURLOPT_CONNECTTIMEOUT, 5L);
    /* Max time in seconds that libcurl transfer operation is allowed to take */
    //curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT, 10L);
    /* Time out for Expect: 100-continue response in milliseconds */
    //curl_easy_setopt(curl_handle, CURLOPT_EXPECT_100_TIMEOUT_MS, 0L);

    /* Enable the cookie engine without reading any initial cookies */
    curl_easy_setopt(curl_handle, CURLOPT_COOKIEFILE, "");
    /* allow whatever auth the proxy speaks */
    curl_easy_setopt(curl_handle, CURLOPT_PROXYAUTH, CURLAUTH_ANY);
    /* allow whatever auth the server speaks */
    curl_easy_setopt(curl_handle, CURLOPT_HTTPAUTH, CURLAUTH_ANY);
}

void* search_url(void* arg) {
    
    while (1) {
        CURL *curl_handle = curl_easy_init();
        CURLcode res;
        pthread_mutex_lock(&crawl_state->mutex);


        // printf("front ptr is : %d", crawl_state->front_ptr);
        while (crawl_state->front_ptr >= crawl_state->back_ptr && crawl_state->done == 0) {

            int t = atomic_fetch_add(&crawl_state->idle_threads, 1);
            if (t+1 >=crawl_state->num_threads)
                break;
            pthread_cond_wait(&crawl_state->cond, &crawl_state->mutex);
            atomic_fetch_add(&crawl_state->idle_threads, -1);
        }     

        
        // if (crawl_state->done == 1) {
        //     pthread_cond_broadcast(&crawl_state->cond);
        //     pthread_mutex_unlock(&crawl_state->mutex);
        //     curl_easy_cleanup(curl_handle);
        //     break;
        // }

        if (atomic_load(&crawl_state->idle_threads) >= crawl_state->num_threads || crawl_state->done) {
            pthread_cond_broadcast(&crawl_state->cond);
            pthread_mutex_unlock(&crawl_state->mutex);
            curl_easy_cleanup(curl_handle);
            break;
        }

        // if (crawl_state->done) {
        //     pthread_cond_broadcast(&crawl_state->cond);
        //     pthread_mutex_unlock(&crawl_state->mutex);
        //     curl_easy_cleanup(curl_handle);
        //     break;
        // }


        if (crawl_state->front_ptr >= MAX_NUM_ITEMS) {
            fprintf(stderr, "BUFFER TOO SMALL\n");
            exit(-1);
        }
        // printf("THE INDEX OF THjE FRONT+PTR IS %d\n", crawl_state->front_ptr);

        char url[256];
        strcpy(url, crawl_state->queue[crawl_state->front_ptr++]);

        pthread_mutex_unlock(&crawl_state->mutex);

        RECV_BUF recv_buf;
        recv_buf_init(&recv_buf, BUF_SIZE);

        /* specify URL to get */
        curl_set_options(curl_handle, &recv_buf);
        curl_easy_setopt(curl_handle, CURLOPT_URL, url);
        res = curl_easy_perform(curl_handle);

        if ( res != CURLE_OK) {
            // printf("Curel perform failed,the url was: %s \n", url);
            // fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
            cleanup(curl_handle, &recv_buf);
            continue;
            // exit(-1);
        }

        long response_code;
        res = curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE, &response_code);

        if (response_code == 400 || response_code == 500) {
            // fprintf(stderr, "Errorcode is 400or 500 \n");
            cleanup(curl_handle, &recv_buf);
            continue;
        }
        
        char *ct = NULL;
        res = curl_easy_getinfo(curl_handle, CURLINFO_CONTENT_TYPE, &ct);
        if (res == CURLE_OK && ct != NULL) {
            // printf("Content-Type: %s, len=%ld\n", ct, strlen(ct));
        } else {
            fprintf(stderr, "Failed obtain Content-Type\n");
            exit(-1);
        }
        
        U8 expected_magic[8] = {0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A};
        if ( strstr(ct, CT_HTML) ) {
            char *link = NULL; 
            int follow_relative_link = 1;
            curl_easy_getinfo(curl_handle, CURLINFO_EFFECTIVE_URL, &link);
            if (crawl_state->url_ptr >= MAX_NUM_ITEMS) {
                fprintf(stderr, "NEED TO ALLOCATE MORE SPACE IN THE BUFFERS, specifically the url_list\n");
                exit(-1);
            }
            pthread_mutex_lock(&crawl_state->url_lock);
            crawl_state->urls_list[crawl_state->url_ptr++] = strdup(link);
            pthread_mutex_unlock(&crawl_state->url_lock);

            pthread_mutex_lock(&crawl_state->parse_links_lock);
            find_http_and_add(recv_buf.buf, recv_buf.size, follow_relative_link, link);   
            pthread_mutex_unlock(&crawl_state->parse_links_lock);
            // if (get_links_res != 0) {

            // }
            // parse_links_from_url_data(recv_buf.buf);
        } 

        else if (recv_buf.size >= 8 && memcmp(recv_buf.buf, expected_magic, 8) == 0 ) {
            char *eurl = NULL;          /* effective URL */
            curl_easy_getinfo(curl_handle, CURLINFO_EFFECTIVE_URL, &eurl);

    
            // if (memcmp(recv_buf.buf, expected_magic, 8) != 0) {
            //     fprintf(stderr, "received a png but it's not valid\n");
            //     for (int i = 0; i < 16 && i < recv_buf.size; i++)
            //         printf("%02X ", (unsigned char)recv_buf.buf[i]);
            //     printf("\n");
            //     printf("the url is %s\n", eurl);
            //     continue;
            //     exit(-1);
            // }
            pthread_mutex_lock(&crawl_state->png_lock);
            if (crawl_state->done) {
                pthread_mutex_unlock(&crawl_state->png_lock);
                cleanup(curl_handle, &recv_buf);
                break;
            }
            //fix this, and write all freed stuff to NULL, so no double free corruption
            sem_wait(&stream_state->empty);
            pthread_mutex_lock(&stream_state->stream_buf_lock);
            crawl_state->pngs_list[stream_state->back] = strdup(eurl);
            printf("Wrote the following url to the buffer: %s\n", crawl_state->pngs_list[stream_state->back]);
            stream_state->num_frames_produced++;
            stream_state->back = (stream_state->back+1) % STREAM_BUFFER_ITEMS;
            pthread_mutex_unlock(&stream_state->stream_buf_lock);
            sem_post(&stream_state->filled);
            if (stream_state->num_frames_produced == crawl_state->target) {
                crawl_state->done = 1;
                pthread_cond_broadcast(&crawl_state->cond);
                pthread_mutex_unlock(&crawl_state->png_lock);
                cleanup(curl_handle, &recv_buf);
                break;
            }
            // crawl_state->target--;
            pthread_mutex_unlock(&crawl_state->png_lock);

            // if target, broadcast or something?
        }
        recv_buf_cleanup(&recv_buf);
        curl_easy_cleanup(curl_handle);
    }
    // curl_easy_cleanup(curl_handle);
    int num_completed_threads = atomic_fetch_add(&crawl_state->threads_done_crawling, 1) + 1;
    if (num_completed_threads == crawl_state->num_threads) {
        sem_post(&crawl_state->done_crawling);
    }

    pthread_mutex_lock(&stream_state->stream_buf_lock);
    stream_state->done = 1;
    pthread_mutex_unlock(&stream_state->stream_buf_lock);
    return NULL;
}

int main(int argc, char* argv[]) {

    if (argc < 2) {
        fprintf(stderr, "Usage: %s options -t (num crawler threads) -m (target num pngs) -v (file showing of URLs) -s (num streaming threads) seed_url\n", argv[0]);
        exit(-1);
    }

    int c;
    int num_threads = 1;
    int target_num_pngs=50;
    char* url_logfile = "";
    char* seed_url;
    int num_cons_stream_threads = 2;

    
    while ((c = getopt (argc, argv, "t:m:v:s:")) != -1) {
        switch (c) {
        case 't':
	        num_threads = strtoul(optarg, NULL, 10);
	        // printf("option -t specifies a value of %d.\n", num_threads);
	        if (num_threads <= 0) {
                fprintf(stderr, "%s: %d must be greater than or equal to 1\n", argv[0], num_threads);
                return -1;
            }
            break;
        case 'm':
            target_num_pngs = strtoul(optarg, NULL, 10);
	        // printf("option -m specifies a value of %d.\n", target_num_pngs);
            if (target_num_pngs < 0) {
                fprintf(stderr, "%s: %d must be greater than 0\n", argv[0], target_num_pngs);
                return -1;
            }
            break;
        case 'v':
	        url_logfile = optarg;
	        // printf("option -v specifies a value of %s.\n", url_logfile);
            break;            
        case 's':
	        num_cons_stream_threads = strtoul(optarg, NULL, 10);
	        // printf("option -s specifies a value of %s.\n", num_cons_stream_threads);
            break;               
        default:
            fprintf(stderr, "Usage: %s -t <number> -f <file>\n", argv[0]);
            return -1;
        }
    }

    seed_url = argv[argc-1];

    // printf("The seed url is %s\n", seed_url);

    hcreate(203 + target_num_pngs); //able to store 200 links
    crawl_state = malloc(sizeof(struct web_crawler_state));

    crawl_state->num_threads = num_threads;
    crawl_state->target = target_num_pngs;
    crawl_state->front_ptr = 0;
    crawl_state->back_ptr = 0;
    crawl_state->png_ptr = 0;
    crawl_state->url_ptr = 0;
    crawl_state->done = 0;
    crawl_state->pngs_list = calloc(target_num_pngs, sizeof(char*));
    pthread_mutex_init(&crawl_state->mutex, NULL);
    pthread_mutex_init(&crawl_state->url_lock, NULL);
    pthread_mutex_init(&crawl_state->png_lock, NULL);
    pthread_mutex_init(&crawl_state->q_lock, NULL);
    pthread_mutex_init(&crawl_state->set_lock, NULL);
    pthread_mutex_init(&crawl_state->parse_links_lock, NULL);
    pthread_cond_init(&crawl_state->cond, NULL);
    sem_init(&crawl_state->done_crawling, 0, 0);
    atomic_init(&crawl_state->threads_done_crawling, 0);
    atomic_init(&crawl_state->idle_threads, 0);
    //to add to set, hsearch(ENTRY item, ACTION action)
    memset(crawl_state->urls_list, 0, sizeof(crawl_state->urls_list));
    memset(crawl_state->queue, 0, sizeof(crawl_state->queue));
    seed_url = strdup(seed_url);
    crawl_state->queue[crawl_state->back_ptr++] = seed_url;
    
    // printf("HELLO\n");
    ENTRY e, *ep;
    e.key = seed_url;
    e.data = NULL;  // Your data pointer
    ep = hsearch(e, ENTER);
    if (ep == NULL) {
        fprintf(stderr, "issue adding the seed_url into the hash set \n");
    }

    //to add hsearch(item, ENTER);
    //to check if its in the set: hsearch(item, FIND), which returns NULL when its not in the set.

    stream_state = init_stream_buffer();


    double times[2];
    struct timeval tv;

    if (gettimeofday(&tv, NULL) != 0) {
        perror("gettimeofday");
        abort();
    }
    times[0] = (tv.tv_sec) + tv.tv_usec/1000000.;

    pthread_t *tids = malloc(sizeof(pthread_t) * num_threads);

    pthread_t *stream_tids = malloc(sizeof(pthread_t)* num_cons_stream_threads);
    
    curl_global_init(CURL_GLOBAL_DEFAULT);

    // printf("DOne initializing stuff, now doing the multithreaded webcrawler \n");

    // printf("href: %s\n", seed_url);
    // crawl_state->urls_list[crawl_state->url_ptr++] = seed_url;

    for (int i =0; i < num_threads;++i) {
        pthread_create(tids+i, NULL, search_url, NULL);
    }

    for (int i =0; i < num_cons_stream_threads; ++i) {
        pthread_create(stream_tids+i, NULL, consumer_thread_func, stream_state);
    }



    sem_wait(&crawl_state->done_crawling);
    xmlCleanupParser();
    // printf("DONE CRAWLING!!!\n");

    for (int i =0; i < num_threads;++i) {
        pthread_join(tids[i], NULL);
    }
    // printf("Check error\n");

    curl_global_cleanup();

    if (gettimeofday(&tv, NULL) != 0) {
        perror("gettimeofday");
        abort();
    }
    times[1] = (tv.tv_sec) + tv.tv_usec/1000000.;

    printf("findpng2 execution time: %.2lf seconds\n", times[1] - times[0]);
    
    if (strcmp(url_logfile, "") != 0) {
        //write the urls to the logfile;
        FILE *fp = fopen(url_logfile, "w");
        if (fp == NULL) {
            fprintf(stdout, "error opening the url logfile\n");
            exit(-1);
        }
        fclose(fp);

        fp = fopen(url_logfile, "a");
        
        // printf("URL PTR IS %d \n", crawl_state->url_ptr);

        for (int i =0; i < crawl_state->url_ptr;++i) {
            if (crawl_state->urls_list[i] ==NULL)
                continue;
            size_t num_bytes_written = fwrite(crawl_state->urls_list[i], 1, strlen(crawl_state->urls_list[i]), fp);
            fwrite("\n", 1, 1, fp); 
            //write to the file, LINE BY LINE
            if (num_bytes_written != strlen(crawl_state->urls_list[i])) {
                fprintf(stderr, "ERROR with url list writing maybe \n");
            }
        }
        fclose(fp);
    }

    FILE *fp = fopen("png_urls.txt", "w");
    if (fp == NULL) {
        fprintf(stderr, "error opening the png logfile\n");
        exit(-1);
    }
    fclose(fp);
    fp = fopen("png_urls.txt", "a");

    // printf("PNG PTR IS %d \n", crawl_state->png_ptr);
    for (int i = 0; i < crawl_state->png_ptr;++i) {
        size_t num_bytes_written = fwrite(crawl_state->pngs_list[i], 1, strlen(crawl_state->pngs_list[i]), fp);
        fwrite("\n", 1, 1, fp); 
        if (num_bytes_written != strlen(crawl_state->pngs_list[i])) {
            fprintf(stderr, "ERROR with png list writing maybe \n");
        }
        //write to the file
    }
    fclose(fp);

    // for (int i =0; i <target_num_pngs;++i) {
    //     xmlFree(crawl_state->pngs_list[i]);
    // }
    for (int i = 0; i < crawl_state->back_ptr; ++i) {

        free(crawl_state->queue[i]); 
        crawl_state->queue[i] = NULL;
    }

    // printf("Debuf cleanup steps\n");
    for (int i = 0; i < crawl_state->png_ptr; ++i) {
        free(crawl_state->pngs_list[i]);
        crawl_state->pngs_list[i] = NULL;
    }
    for (int i = 0; i < crawl_state->url_ptr; ++i) {
        free(crawl_state->urls_list[i]);
        crawl_state->urls_list[i] = NULL;
    }

    free(tids);
    tids = NULL;
    free(crawl_state->pngs_list);
    crawl_state->pngs_list = NULL;
    pthread_mutex_destroy(&crawl_state->mutex);
    pthread_mutex_destroy(&crawl_state->url_lock);
    pthread_mutex_destroy(&crawl_state->png_lock);
    pthread_mutex_destroy(&crawl_state->q_lock);
    pthread_mutex_destroy(&crawl_state->parse_links_lock);
    pthread_mutex_destroy(&crawl_state->set_lock);
    pthread_cond_destroy(&crawl_state->cond);
    sem_destroy(&crawl_state->done_crawling);
    // hdestroy();
    free(crawl_state);
    crawl_state = NULL;

   pthread_mutex_destroy(&stream_state->stream_buf_lock);
   sem_destroy(&stream_state->filled);
   sem_destroy(&stream_state->empty);
   free(stream_state->urls);
   free(stream_state->pngs);
   free(stream_state->sizes);
   free(stream_state);
    
    return 0;
}