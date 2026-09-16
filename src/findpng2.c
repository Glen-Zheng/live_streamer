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
#include <sys/time.h>
#include <signal.h>
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
    char **urls;
    U8 **pngs;
    size_t *sizes;
    int back;
    int front;
    int num_frames;
    int num_frames_produced;
    int done;
    pthread_mutex_t stream_buf_lock;
    sem_t filled;
    sem_t empty;
} stream_buf_state;

stream_buf_state* stream_state;


/* ============================================================
 * TIMING / FFMPEG SUPPORT
 * ============================================================
 */

#define MAX_TIMED_FRAMES MAX_NUM_ITEMS

typedef struct {
    double download_complete_time;
    double encoded_ready_time;

    bool download_complete;
    bool encoded_ready;
    bool reported;
} frame_timing;

typedef struct {
    frame_timing *timings;
    int max_frames;
    pthread_mutex_t *timing_lock;
    int progress_fd;
} progress_thread_args;

typedef struct {
    pid_t pid;

    /*
     * Parent writes PNG bytes here.
     * This is FFmpeg's stdin.
     */
    FILE *stdin_pipe;

    /*
     * Parent reads FFmpeg progress here.
     */
    int progress_fd;

    pthread_t progress_thread;

    frame_timing *timings;
    int max_frames;

    pthread_mutex_t timing_lock;
} ffmpeg_process;


/*
 * Monotonic clock:
 * unaffected by changes to wall-clock time.
 */
static double now_sec(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        perror("clock_gettime");
        return 0.0;
    }

    return (double)ts.tv_sec +
           (double)ts.tv_nsec / 1000000000.0;
}


/*
 * FFmpeg writes progress information such as:
 *
 * frame=1
 * fps=...
 * stream_0_0_q=...
 * ...
 *
 * We use frame=N to identify which input frame FFmpeg has
 * processed.
 */
static void *ffmpeg_progress_thread(void *arg)
{
    progress_thread_args *args =
        (progress_thread_args *)arg;

    FILE *progress =
        fdopen(args->progress_fd, "r");

    if (progress == NULL) {
        fprintf(stderr,
                "[timing] fdopen failed: %s\n",
                strerror(errno));

        close(args->progress_fd);
        free(args);

        return NULL;
    }

    char line[256];

    while (fgets(line, sizeof(line), progress) != NULL) {

        int ffmpeg_frame;

        if (sscanf(line, "frame=%d", &ffmpeg_frame) != 1) {
            continue;
        }

        /*
         * FFmpeg frame numbering starts at 1.
         * Our frame numbering starts at 0.
         */
        int frame_idx = ffmpeg_frame - 1;

        if (frame_idx < 0 ||
            frame_idx >= args->max_frames) {
            continue;
        }

        double encoded_time = now_sec();

        pthread_mutex_lock(args->timing_lock);

        args->timings[frame_idx].encoded_ready_time =
            encoded_time;

        args->timings[frame_idx].encoded_ready =
            true;

        /*
         * Normally the PNG download has already completed.
         *
         * However, FFmpeg can be faster than the curl call
         * returning to the caller, so handle either ordering.
         */
        if (args->timings[frame_idx].download_complete &&
            !args->timings[frame_idx].reported) {

            double latency =
                args->timings[frame_idx].encoded_ready_time -
                args->timings[frame_idx].download_complete_time;

            fprintf(stderr,
                    "[LATENCY] Frame %d: %.3f ms\n",
                    frame_idx,
                    latency * 1000.0);

            args->timings[frame_idx].reported = true;
        }

        pthread_mutex_unlock(args->timing_lock);
    }

    fclose(progress);

    free(args);

    return NULL;
}


/*
 * Start FFmpeg:
 *
 * Parent
 *   |
 *   | PNG bytes
 *   v
 * FFmpeg stdin
 *
 * FFmpeg
 *   |
 *   | progress
 *   v
 * Parent progress pipe
 */
static int start_ffmpeg(ffmpeg_process *proc)
{
    int stdin_pipe[2];
    int progress_pipe[2];

    if (pipe(stdin_pipe) != 0) {
        fprintf(stderr,
                "pipe(stdin) failed: %s\n",
                strerror(errno));

        return -1;
    }

    if (pipe(progress_pipe) != 0) {
        fprintf(stderr,
                "pipe(progress) failed: %s\n",
                strerror(errno));

        close(stdin_pipe[0]);
        close(stdin_pipe[1]);

        return -1;
    }

    pid_t pid = fork();

    if (pid < 0) {

        fprintf(stderr,
                "fork failed: %s\n",
                strerror(errno));

        close(stdin_pipe[0]);
        close(stdin_pipe[1]);
        close(progress_pipe[0]);
        close(progress_pipe[1]);

        return -1;
    }

    /*
     * CHILD
     */
    if (pid == 0) {

        /*
         * stdin = PNG input
         */
        if (dup2(stdin_pipe[0], STDIN_FILENO) < 0) {
            perror("dup2(stdin)");
            _exit(1);
        }

        /*
         * FD 3 = FFmpeg progress output
         */
        if (dup2(progress_pipe[1], 3) < 0) {
            perror("dup2(progress)");
            _exit(1);
        }

        close(stdin_pipe[0]);
        close(stdin_pipe[1]);

        close(progress_pipe[0]);
        close(progress_pipe[1]);


        /*
         * Preserve the old FFmpeg log.
         */
        int log_fd =
            open("/tmp/ffmpeg_log1.txt",
                 O_WRONLY | O_CREAT | O_TRUNC,
                 0644);

        if (log_fd < 0) {
            fprintf(stderr,
                    "open ffmpeg log failed: %s\n",
                    strerror(errno));

            _exit(1);
        }

        if (dup2(log_fd, STDERR_FILENO) < 0) {
            perror("dup2(stderr)");

            close(log_fd);

            _exit(1);
        }

        close(log_fd);


        /*
         * Same FFmpeg pipeline you had before.
         *
         * Added:
         *
         *   -progress pipe:3
         *   -stats_period 0.01
         *
         * so the parent can see frame=N updates.
         */
        execl(
            "/app/ffmpeg",
            "/app/ffmpeg",

            "-y",

            "-fflags",
            "nobuffer",

            "-flags",
            "low_delay",

            "-f",
            "image2pipe",

            "-vcodec",
            "png",

            "-framerate",
            "1",

            "-i",
            "pipe:0",

            "-c:v",
            "mpeg2video",

            "-q:v",
            "5",

            "-progress",
            "pipe:3",

            "-stats_period",
            "0.01",

            "-f",
            "mpegts",

            "tcp://127.0.0.1:1234",

            NULL
        );

        fprintf(stderr,
                "execl(/app/ffmpeg) failed: %s\n",
                strerror(errno));

        _exit(1);
    }


    /*
     * PARENT
     */

    close(stdin_pipe[0]);
    close(progress_pipe[1]);


    FILE *ffmpeg_stdin =
        fdopen(stdin_pipe[1], "w");

    if (ffmpeg_stdin == NULL) {

        fprintf(stderr,
                "fdopen(stdin) failed: %s\n",
                strerror(errno));

        close(stdin_pipe[1]);
        close(progress_pipe[0]);

        kill(pid, SIGTERM);

        waitpid(pid, NULL, 0);

        return -1;
    }


    /*
     * Do not buffer PNG bytes in stdio.
     */
    setvbuf(
        ffmpeg_stdin,
        NULL,
        _IONBF,
        0
    );


    proc->pid = pid;
    proc->stdin_pipe = ffmpeg_stdin;
    proc->progress_fd = progress_pipe[0];


    return 0;
}


static int start_progress_thread(ffmpeg_process *proc)
{
    progress_thread_args *args =
        malloc(sizeof(progress_thread_args));

    if (args == NULL) {
        fprintf(stderr,
                "malloc progress args failed\n");

        return -1;
    }

    args->timings = proc->timings;
    args->max_frames = proc->max_frames;
    args->timing_lock = &proc->timing_lock;
    args->progress_fd = proc->progress_fd;


    if (pthread_create(
            &proc->progress_thread,
            NULL,
            ffmpeg_progress_thread,
            args
        ) != 0) {

        fprintf(stderr,
                "pthread_create(progress) failed\n");

        free(args);

        close(proc->progress_fd);

        return -1;
    }

    return 0;
}


static void stop_ffmpeg(ffmpeg_process *proc)
{
    if (proc == NULL) {
        return;
    }

    /*
     * Closing stdin tells FFmpeg there are no more PNGs.
     */
    if (proc->stdin_pipe != NULL) {

        fflush(proc->stdin_pipe);

        fclose(proc->stdin_pipe);

        proc->stdin_pipe = NULL;
    }


    /*
     * Wait for FFmpeg's progress thread to see EOF.
     */
    pthread_join(
        proc->progress_thread,
        NULL
    );


    int status;

    waitpid(
        proc->pid,
        &status,
        0
    );


    if (WIFEXITED(status)) {

        fprintf(stderr,
                "[ffmpeg] exited with status %d\n",
                WEXITSTATUS(status));

    } else if (WIFSIGNALED(status)) {

        fprintf(stderr,
                "[ffmpeg] killed by signal %d\n",
                WTERMSIG(status));
    }
}


/* ============================================================
 * ORIGINAL CODE
 * ============================================================
 */

stream_buf_state* init_stream_buffer() {

    stream_buf_state *buf =
        malloc(sizeof(stream_buf_state));

    buf->urls =
        malloc(STREAM_BUFFER_ITEMS *
               sizeof(char*));

    buf->pngs =
        malloc(STREAM_BUFFER_ITEMS *
               sizeof(U8*));

    buf->sizes =
        malloc(STREAM_BUFFER_ITEMS *
               sizeof(size_t));

    buf->front = 0;
    buf->back = 0;
    buf->num_frames = 0;
    buf->num_frames_produced = 0;
    buf->done = 0;

    pthread_mutex_init(
        &buf->stream_buf_lock,
        NULL
    );

    sem_init(
        &buf->filled,
        0,
        0
    );

    sem_init(
        &buf->empty,
        0,
        STREAM_BUFFER_ITEMS
    );

    return buf;
}


static size_t stream_write_callback(
    void *contents,
    size_t size,
    size_t nmemb,
    void *userp
) {

    size_t realsize =
        size * nmemb;

    FILE *pipe =
        (FILE *)userp;

    size_t written =
        fwrite(
            contents,
            size,
            nmemb,
            pipe
        );

    fprintf(
        stderr,
        "[curl] wrote %zu bytes to pipe\n",
        written * size
    );


    if (written != nmemb) {

        fprintf(
            stderr,
            "Error: Failed to write to FFmpeg pipe\n"
        );

        return 0;
    }

    return realsize;
}


void* consumer_thread_func(void *arg)
{
    stream_buf_state *buf =
        (stream_buf_state *)arg;


    /*
     * ------------------------------------------------------------
     * TIMING STATE
     * ------------------------------------------------------------
     */

    ffmpeg_process ffmpeg;

    memset(
        &ffmpeg,
        0,
        sizeof(ffmpeg)
    );


    ffmpeg.max_frames =
        MAX_TIMED_FRAMES;


    ffmpeg.timings =
        calloc(
            ffmpeg.max_frames,
            sizeof(frame_timing)
        );


    if (ffmpeg.timings == NULL) {

        fprintf(
            stderr,
            "Failed to allocate frame timing array\n"
        );

        return NULL;
    }


    pthread_mutex_init(
        &ffmpeg.timing_lock,
        NULL
    );


    /*
     * ------------------------------------------------------------
     * START FFMPEG
     * ------------------------------------------------------------
     */

    if (start_ffmpeg(&ffmpeg) != 0) {

        pthread_mutex_destroy(
            &ffmpeg.timing_lock
        );

        free(ffmpeg.timings);

        return NULL;
    }


    if (start_progress_thread(&ffmpeg) != 0) {

        fclose(
            ffmpeg.stdin_pipe
        );

        waitpid(
            ffmpeg.pid,
            NULL,
            0
        );

        pthread_mutex_destroy(
            &ffmpeg.timing_lock
        );

        free(ffmpeg.timings);

        return NULL;
    }


    /*
     * ------------------------------------------------------------
     * ORIGINAL CONSUMER LOOP
     * ------------------------------------------------------------
     */

    while (1) {

        CURL *curl =
            curl_easy_init();


        if (!curl) {

            fprintf(
                stderr,
                "Error: CURL init failed on consumer stream thread\n"
            );

            continue;
        }


        if (
            buf->num_frames ==
            buf->num_frames_produced &&
            buf->done
        ) {

            curl_easy_cleanup(curl);

            break;
        }


        sem_wait(
            &buf->filled
        );


        pthread_mutex_lock(
            &buf->stream_buf_lock
        );


        char *url =
            crawl_state->pngs_list[
                buf->front
            ];


        fprintf(
            stderr,
            "[Consumer] Got: %s \n",
            url
        );


        /*
         * Existing frame number.
         */
        int frame_num =
            buf->num_frames++;


        buf->front =
            (buf->front + 1) %
            STREAM_BUFFER_ITEMS;


        sem_post(
            &buf->empty
        );


        pthread_mutex_unlock(
            &buf->stream_buf_lock
        );


        /*
         * --------------------------------------------------------
         * DOWNLOAD PNG
         * --------------------------------------------------------
         */

        curl_easy_setopt(
            curl,
            CURLOPT_URL,
            url
        );


        curl_easy_setopt(
            curl,
            CURLOPT_WRITEFUNCTION,
            stream_write_callback
        );


        /*
         * PNG bytes go directly to FFmpeg stdin.
         */
        curl_easy_setopt(
            curl,
            CURLOPT_WRITEDATA,
            (void *)ffmpeg.stdin_pipe
        );


        curl_easy_setopt(
            curl,
            CURLOPT_TIMEOUT,
            30L
        );


        curl_easy_setopt(
            curl,
            CURLOPT_FOLLOWLOCATION,
            1L
        );


        CURLcode res =
            curl_easy_perform(curl);


        fprintf(
            stderr,
            "[curl] perform result: %s\n",
            curl_easy_strerror(res)
        );


        long http_code;

        curl_easy_getinfo(
            curl,
            CURLINFO_RESPONSE_CODE,
            &http_code
        );


        fprintf(
            stderr,
            "[curl] HTTP status: %ld\n",
            http_code
        );


        if (res == CURLE_OK) {

            fflush(
                ffmpeg.stdin_pipe
            );


            /*
             * ====================================================
             * T0:
             *
             * PNG is now fully downloaded.
             *
             * We intentionally do NOT include PNG download time.
             * ====================================================
             */

            double download_complete =
                now_sec();


            pthread_mutex_lock(
                &ffmpeg.timing_lock
            );


            ffmpeg.timings[
                frame_num
            ].download_complete_time =
                download_complete;


            ffmpeg.timings[
                frame_num
            ].download_complete =
                true;


            /*
             * FFmpeg may have already reported the encoded frame.
             *
             * If so, calculate the latency now.
             */
            if (
                ffmpeg.timings[
                    frame_num
                ].encoded_ready &&
                !ffmpeg.timings[
                    frame_num
                ].reported
            ) {

                double latency =
                    ffmpeg.timings[
                        frame_num
                    ].encoded_ready_time
                    -
                    ffmpeg.timings[
                        frame_num
                    ].download_complete_time;


                fprintf(
                    stderr,
                    "[LATENCY] Frame %d: %.3f ms\n",
                    frame_num,
                    latency * 1000.0
                );


                ffmpeg.timings[
                    frame_num
                ].reported =
                    true;
            }


            pthread_mutex_unlock(
                &ffmpeg.timing_lock
            );


            fprintf(
                stderr,
                "[Consumer] Frame %d: Downloaded and piped %s\n",
                frame_num,
                url
            );

        } else {

            fprintf(
                stderr,
                "[Consumer] Error downloading %s: %s\n",
                url,
                curl_easy_strerror(res)
            );
        }


        free(url);

        curl_easy_cleanup(curl);
    }


    fprintf(
        stderr,
        "Closing ffmpeg pipe...\n"
    );


    fflush(
        ffmpeg.stdin_pipe
    );


    sleep(2);


    stop_ffmpeg(
        &ffmpeg
    );


    fprintf(
        stderr,
        "ffmpeg pipe closed\n"
    );


    fprintf(
        stderr,
        "[✓] Video complete: %d frames\n",
        buf->num_frames
    );


    pthread_mutex_destroy(
        &ffmpeg.timing_lock
    );


    free(
        ffmpeg.timings
    );


    return NULL;
}



int find_http_and_add(
    char *buf,
    int size,
    int follow_relative_links,
    const char *base_url
) {

    int i;

    htmlDocPtr doc;

    xmlChar *xpath =
        (xmlChar*) "//a/@href";

    xmlNodeSetPtr nodeset;

    xmlXPathObjectPtr result;

    xmlChar *href;


    if (buf == NULL) {
        return -1;
    }


    doc =
        mem_getdoc(
            buf,
            size,
            base_url
        );


    if (doc == NULL)
        return -1;


    result =
        getnodeset(
            doc,
            xpath
        );


    if (result == NULL) {

        xmlFreeDoc(doc);

        return -1;
    }


    if (result) {

        nodeset =
            result->nodesetval;


        for (
            i = 0;
            i < nodeset->nodeNr;
            i++
        ) {

            href =
                xmlNodeListGetString(
                    doc,
                    nodeset->nodeTab[i]->xmlChildrenNode,
                    1
                );


            if (follow_relative_links) {

                xmlChar *old =
                    href;

                href =
                    xmlBuildURI(
                        href,
                        (xmlChar *)base_url
                    );

                xmlFree(old);
            }


            if (
                href != NULL &&
                !strncmp(
                    (const char *)href,
                    "http",
                    4
                )
            ) {

                char *href_copy =
                    strdup((char*)href);

                xmlFree(href);


                ENTRY e, *ep;

                e.key = href_copy;

                e.data = NULL;


                pthread_mutex_lock(
                    &crawl_state->set_lock
                );


                ep =
                    hsearch(
                        e,
                        FIND
                    );


                if (ep == NULL) {

                    hsearch(
                        e,
                        ENTER
                    );


                    if (
                        crawl_state->back_ptr >=
                        MAX_NUM_ITEMS
                    ) {

                        pthread_mutex_unlock(
                            &crawl_state->set_lock
                        );


                        fprintf(
                            stderr,
                            "NEED TO ALLOCATE MORE SPACE IN THE BUFFERS, specifically in the queue - No longer adding new URLS to crawl!\n"
                        );


                        return -2;
                    }


                    pthread_mutex_lock(
                        &crawl_state->mutex
                    );


                    crawl_state->queue[
                        crawl_state->back_ptr++
                    ] =
                        href_copy;


                    pthread_mutex_unlock(
                        &crawl_state->mutex
                    );


                    pthread_cond_signal(
                        &crawl_state->cond
                    );

                } else {

                    free(href_copy);

                    href_copy = NULL;
                }


                pthread_mutex_unlock(
                    &crawl_state->set_lock
                );

            } else {

                xmlFree(href);
            }
        }


        xmlXPathFreeObject(result);
    }


    xmlFreeDoc(doc);

    return 0;
}



void curl_set_options(
    CURL *curl_handle,
    RECV_BUF* ptr
) {

    curl_easy_setopt(
        curl_handle,
        CURLOPT_WRITEFUNCTION,
        write_cb_curl3
    );


    curl_easy_setopt(
        curl_handle,
        CURLOPT_WRITEDATA,
        (void *)ptr
    );


    curl_easy_setopt(
        curl_handle,
        CURLOPT_HEADERFUNCTION,
        header_cb_curl
    );


    curl_easy_setopt(
        curl_handle,
        CURLOPT_HEADERDATA,
        (void *)ptr
    );


    curl_easy_setopt(
        curl_handle,
        CURLOPT_USERAGENT,
        "ece252 lab4 crawler"
    );


    curl_easy_setopt(
        curl_handle,
        CURLOPT_FOLLOWLOCATION,
        1L
    );


    curl_easy_setopt(
        curl_handle,
        CURLOPT_UNRESTRICTED_AUTH,
        1L
    );


    curl_easy_setopt(
        curl_handle,
        CURLOPT_MAXREDIRS,
        5L
    );


    curl_easy_setopt(
        curl_handle,
        CURLOPT_ACCEPT_ENCODING,
        ""
    );


    curl_easy_setopt(
        curl_handle,
        CURLOPT_COOKIEFILE,
        ""
    );


    curl_easy_setopt(
        curl_handle,
        CURLOPT_PROXYAUTH,
        CURLAUTH_ANY
    );


    curl_easy_setopt(
        curl_handle,
        CURLOPT_HTTPAUTH,
        CURLAUTH_ANY
    );
}



void* search_url(void* arg)
{
    while (1) {

        CURL *curl_handle =
            curl_easy_init();

        CURLcode res;

        pthread_mutex_lock(
            &crawl_state->mutex
        );


        while (
            crawl_state->front_ptr >=
            crawl_state->back_ptr &&
            crawl_state->done == 0
        ) {

            int t =
                atomic_fetch_add(
                    &crawl_state->idle_threads,
                    1
                );


            if (
                t + 1 >=
                crawl_state->num_threads
            )
                break;


            pthread_cond_wait(
                &crawl_state->cond,
                &crawl_state->mutex
            );


            atomic_fetch_add(
                &crawl_state->idle_threads,
                -1
            );
        }


        if (
            atomic_load(
                &crawl_state->idle_threads
            ) >=
            crawl_state->num_threads ||
            crawl_state->done
        ) {

            pthread_cond_broadcast(
                &crawl_state->cond
            );


            pthread_mutex_unlock(
                &crawl_state->mutex
            );


            curl_easy_cleanup(
                curl_handle
            );


            break;
        }


        if (
            crawl_state->front_ptr >=
            MAX_NUM_ITEMS
        ) {

            fprintf(
                stderr,
                "BUFFER TOO SMALL\n"
            );

            exit(-1);
        }


        char url[256];


        strcpy(
            url,
            crawl_state->queue[
                crawl_state->front_ptr++
            ]
        );


        pthread_mutex_unlock(
            &crawl_state->mutex
        );


        RECV_BUF recv_buf;


        recv_buf_init(
            &recv_buf,
            BUF_SIZE
        );


        curl_set_options(
            curl_handle,
            &recv_buf
        );


        curl_easy_setopt(
            curl_handle,
            CURLOPT_URL,
            url
        );


        res =
            curl_easy_perform(
                curl_handle
            );


        if (res != CURLE_OK) {

            cleanup(
                curl_handle,
                &recv_buf
            );

            continue;
        }


        long response_code;


        curl_easy_getinfo(
            curl_handle,
            CURLINFO_RESPONSE_CODE,
            &response_code
        );


        if (
            response_code == 400 ||
            response_code == 500
        ) {

            cleanup(
                curl_handle,
                &recv_buf
            );

            continue;
        }


        char *ct = NULL;


        res =
            curl_easy_getinfo(
                curl_handle,
                CURLINFO_CONTENT_TYPE,
                &ct
            );


        if (
            res != CURLE_OK ||
            ct == NULL
        ) {

            fprintf(
                stderr,
                "Failed obtain Content-Type\n"
            );

            exit(-1);
        }


        U8 expected_magic[8] = {
            0x89,
            0x50,
            0x4E,
            0x47,
            0x0D,
            0x0A,
            0x1A,
            0x0A
        };


        if (strstr(ct, CT_HTML)) {

            char *link = NULL;

            int follow_relative_link = 1;


            curl_easy_getinfo(
                curl_handle,
                CURLINFO_EFFECTIVE_URL,
                &link
            );


            if (
                crawl_state->url_ptr >=
                MAX_NUM_ITEMS
            ) {

                fprintf(
                    stderr,
                    "NEED TO ALLOCATE MORE SPACE IN THE BUFFERS, specifically the url_list\n"
                );

                exit(-1);
            }


            pthread_mutex_lock(
                &crawl_state->url_lock
            );


            crawl_state->urls_list[
                crawl_state->url_ptr++
            ] =
                strdup(link);


            pthread_mutex_unlock(
                &crawl_state->url_lock
            );


            pthread_mutex_lock(
                &crawl_state->parse_links_lock
            );


            find_http_and_add(
                recv_buf.buf,
                recv_buf.size,
                follow_relative_link,
                link
            );


            pthread_mutex_unlock(
                &crawl_state->parse_links_lock
            );
        }


        else if (
            recv_buf.size >= 8 &&
            memcmp(
                recv_buf.buf,
                expected_magic,
                8
            ) == 0
        ) {

            char *eurl = NULL;


            curl_easy_getinfo(
                curl_handle,
                CURLOPT_EFFECTIVE_URL,
                &eurl
            );


            pthread_mutex_lock(
                &crawl_state->png_lock
            );


            if (crawl_state->done) {

                pthread_mutex_unlock(
                    &crawl_state->png_lock
                );


                cleanup(
                    curl_handle,
                    &recv_buf
                );


                break;
            }


            sem_wait(
                &stream_state->empty
            );


            pthread_mutex_lock(
                &stream_state->stream_buf_lock
            );


            crawl_state->pngs_list[
                stream_state->back
            ] =
                strdup(eurl);


            fprintf(
                stderr,
                "Wrote the following url to the buffer: %s\n",
                crawl_state->pngs_list[
                    stream_state->back
                ]
            );


            stream_state->num_frames_produced++;


            stream_state->back =
                (stream_state->back + 1) %
                STREAM_BUFFER_ITEMS;


            pthread_mutex_unlock(
                &stream_state->stream_buf_lock
            );


            sem_post(
                &stream_state->filled
            );


            if (
                stream_state->num_frames_produced ==
                crawl_state->target
            ) {

                crawl_state->done = 1;


                pthread_cond_broadcast(
                    &crawl_state->cond
                );


                pthread_mutex_unlock(
                    &crawl_state->png_lock
                );


                cleanup(
                    curl_handle,
                    &recv_buf
                );


                break;
            }


            pthread_mutex_unlock(
                &crawl_state->png_lock
            );
        }


        recv_buf_cleanup(
            &recv_buf
        );


        curl_easy_cleanup(
            curl_handle
        );
    }


    int num_completed_threads =
        atomic_fetch_add(
            &crawl_state->threads_done_crawling,
            1
        ) + 1;


    if (
        num_completed_threads ==
        crawl_state->num_threads
    ) {

        sem_post(
            &crawl_state->done_crawling
        );
    }


    pthread_mutex_lock(
        &stream_state->stream_buf_lock
    );


    stream_state->done = 1;


    pthread_mutex_unlock(
        &stream_state->stream_buf_lock
    );


    return NULL;
}



int main(
    int argc,
    char* argv[]
)
{
    if (argc < 2) {

        fprintf(
            stderr,
            "Usage: %s options -t (num crawler threads) -m (target num pngs) -v (file showing of URLs) -s (num streaming threads) seed_url\n",
            argv[0]
        );

        exit(-1);
    }


    int c;

    int num_threads = 1;

    int target_num_pngs = 50;

    char* url_logfile = "";

    char* seed_url;

    int num_cons_stream_threads = 2;


    while (
        (c = getopt(
            argc,
            argv,
            "t:m:v:s:"
        )) != -1
    ) {

        switch (c) {

        case 't':

            num_threads =
                strtoul(
                    optarg,
                    NULL,
                    10
                );


            if (num_threads <= 0) {

                fprintf(
                    stderr,
                    "%s: %d must be greater than or equal to 1\n",
                    argv[0],
                    num_threads
                );

                return -1;
            }

            break;


        case 'm':

            target_num_pngs =
                strtoul(
                    optarg,
                    NULL,
                    10
                );


            if (target_num_pngs < 0) {

                fprintf(
                    stderr,
                    "%s: %d must be greater than 0\n",
                    argv[0],
                    target_num_pngs
                );

                return -1;
            }

            break;


        case 'v':

            url_logfile = optarg;

            break;


        case 's':

            num_cons_stream_threads =
                strtoul(
                    optarg,
                    NULL,
                    10
                );

            break;


        default:

            fprintf(
                stderr,
                "Usage: %s -t <number> -f <file>\n",
                argv[0]
            );

            return -1;
        }
    }


    seed_url =
        argv[argc - 1];


    hcreate(
        203 + target_num_pngs
    );


    crawl_state =
        malloc(
            sizeof(struct web_crawler_state)
        );


    crawl_state->num_threads =
        num_threads;

    crawl_state->target =
        target_num_pngs;

    crawl_state->front_ptr = 0;
    crawl_state->back_ptr = 0;
    crawl_state->png_ptr = 0;
    crawl_state->url_ptr = 0;
    crawl_state->done = 0;


    crawl_state->pngs_list =
        calloc(
            target_num_pngs,
            sizeof(char*)
        );


    pthread_mutex_init(
        &crawl_state->mutex,
        NULL
    );


    pthread_mutex_init(
        &crawl_state->url_lock,
        NULL
    );


    pthread_mutex_init(
        &crawl_state->png_lock,
        NULL
    );


    pthread_mutex_init(
        &crawl_state->q_lock,
        NULL
    );


    pthread_mutex_init(
        &crawl_state->set_lock,
        NULL
    );


    pthread_mutex_init(
        &crawl_state->parse_links_lock,
        NULL
    );


    pthread_cond_init(
        &crawl_state->cond,
        NULL
    );


    sem_init(
        &crawl_state->done_crawling,
        0,
        0
    );


    atomic_init(
        &crawl_state->threads_done_crawling,
        0
    );


    atomic_init(
        &crawl_state->idle_threads,
        0
    );


    memset(
        crawl_state->urls_list,
        0,
        sizeof(crawl_state->urls_list)
    );


    memset(
        crawl_state->queue,
        0,
        sizeof(crawl_state->queue)
    );


    seed_url =
        strdup(seed_url);


    crawl_state->queue[
        crawl_state->back_ptr++
    ] =
        seed_url;


    ENTRY e, *ep;


    e.key = seed_url;
    e.data = NULL;


    ep =
        hsearch(
            e,
            ENTER
        );


    if (ep == NULL) {

        fprintf(
            stderr,
            "issue adding the seed_url into the hash set \n"
        );
    }


    stream_state =
        init_stream_buffer();


    double times[2];

    struct timeval tv;


    if (
        gettimeofday(
            &tv,
            NULL
        ) != 0
    ) {

        perror("gettimeofday");

        abort();
    }


    times[0] =
        (tv.tv_sec) +
        tv.tv_usec / 1000000.;


    pthread_t *tids =
        malloc(
            sizeof(pthread_t) *
            num_threads
        );


    pthread_t *stream_tids =
        malloc(
            sizeof(pthread_t) *
            num_cons_stream_threads
        );


    curl_global_init(
        CURL_GLOBAL_DEFAULT
    );


    for (
        int i = 0;
        i < num_threads;
        ++i
    ) {

        pthread_create(
            tids + i,
            NULL,
            search_url,
            NULL
        );
    }


    for (
        int i = 0;
        i < num_cons_stream_threads;
        ++i
    ) {

        pthread_create(
            stream_tids + i,
            NULL,
            consumer_thread_func,
            stream_state
        );
    }


    sem_wait(
        &crawl_state->done_crawling
    );


    xmlCleanupParser();


    for (
        int i = 0;
        i < num_threads;
        ++i
    ) {

        pthread_join(
            tids[i],
            NULL
        );
    }


    for (
        int i = 0;
        i < num_cons_stream_threads;
        ++i
    ) {

        pthread_join(
            stream_tids[i],
            NULL
        );
    }


    curl_global_cleanup();


    if (
        gettimeofday(
            &tv,
            NULL
        ) != 0
    ) {

        perror("gettimeofday");

        abort();
    }


    times[1] =
        (tv.tv_sec) +
        tv.tv_usec / 1000000.;


    fprintf(
        stderr,
        "findpng2 execution time: %.2lf seconds\n",
        times[1] - times[0]
    );


    if (
        strcmp(
            url_logfile,
            ""
        ) != 0
    ) {

        FILE *fp =
            fopen(
                url_logfile,
                "w"
            );


        if (fp == NULL) {

            fprintf(
                stdout,
                "error opening the url logfile\n"
            );

            exit(-1);
        }


        fclose(fp);


        fp =
            fopen(
                url_logfile,
                "a"
            );


        for (
            int i = 0;
            i < crawl_state->url_ptr;
            ++i
        ) {

            if (
                crawl_state->urls_list[i] == NULL
            )
                continue;


            size_t num_bytes_written =
                fwrite(
                    crawl_state->urls_list[i],
                    1,
                    strlen(
                        crawl_state->urls_list[i]
                    ),
                    fp
                );


            fwrite(
                "\n",
                1,
                1,
                fp
            );


            if (
                num_bytes_written !=
                strlen(
                    crawl_state->urls_list[i]
                )
            ) {

                fprintf(
                    stderr,
                    "ERROR with url list writing maybe \n"
                );
            }
        }


        fclose(fp);
    }


    FILE *fp =
        fopen(
            "png_urls.txt",
            "w"
        );


    if (fp == NULL) {

        fprintf(
            stderr,
            "error opening the png logfile\n"
        );

        exit(-1);
    }


    fclose(fp);


    fp =
        fopen(
            "png_urls.txt",
            "a"
        );


    for (
        int i = 0;
        i < crawl_state->png_ptr;
        ++i
    ) {

        size_t num_bytes_written =
            fwrite(
                crawl_state->pngs_list[i],
                1,
                strlen(
                    crawl_state->pngs_list[i]
                ),
                fp
            );


        fwrite(
            "\n",
            1,
            1,
            fp
        );


        if (
            num_bytes_written !=
            strlen(
                crawl_state->pngs_list[i]
            )
        ) {

            fprintf(
                stderr,
                "ERROR with png list writing maybe \n"
            );
        }
    }


    fclose(fp);


    for (
        int i = 0;
        i < crawl_state->back_ptr;
        ++i
    ) {

        free(
            crawl_state->queue[i]
        );

        crawl_state->queue[i] = NULL;
    }


    for (
        int i = 0;
        i < crawl_state->png_ptr;
        ++i
    ) {

        free(
            crawl_state->pngs_list[i]
        );

        crawl_state->pngs_list[i] = NULL;
    }


    for (
        int i = 0;
        i < crawl_state->url_ptr;
        ++i
    ) {

        free(
            crawl_state->urls_list[i]
        );

        crawl_state->urls_list[i] = NULL;
    }


    free(tids);

    tids = NULL;


    free(
        crawl_state->pngs_list
    );

    crawl_state->pngs_list = NULL;


    pthread_mutex_destroy(
        &crawl_state->mutex
    );

    pthread_mutex_destroy(
        &crawl_state->url_lock
    );

    pthread_mutex_destroy(
        &crawl_state->png_lock
    );

    pthread_mutex_destroy(
        &crawl_state->q_lock
    );

    pthread_mutex_destroy(
        &crawl_state->parse_links_lock
    );

    pthread_mutex_destroy(
        &crawl_state->set_lock
    );

    pthread_cond_destroy(
        &crawl_state->cond
    );

    sem_destroy(
        &crawl_state->done_crawling
    );


    free(crawl_state);

    crawl_state = NULL;


    pthread_mutex_destroy(
        &stream_state->stream_buf_lock
    );


    sem_destroy(
        &stream_state->filled
    );


    sem_destroy(
        &stream_state->empty
    );


    free(stream_state->urls);
    free(stream_state->pngs);
    free(stream_state->sizes);

    free(stream_state);


    return 0;
}
