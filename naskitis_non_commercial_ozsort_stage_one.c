/*******************************************************************************
 * // Begin statement                                                          *
 *                                                                             *
 * Author:            Dr. Nikolas Askitis                                      *
 * Email:             askitisn@gmail.com                                       *
 * Github.com:        https://github.com/naskitis                              *
 * Sortbenchmark.org: http://sortbenchmark.org/ozsort-2010.pdf                 *
 *                                                                             *
 * Copyright @ 2016.  All rights reserved.                                     *
 *                                                                             *
 * Permission to use my software is granted provided that this statement       *
 * is retained.                                                                *
 *                                                                             *
 * My software is for non-commercial use only.                                 *
 *                                                                             *
 * If you want to share my software with others, please do so by               *
 * sharing a link to my repository on github.com.                              *
 *                                                                             *
 * If you would like to use any part of my software in a commercial or public  *
 * environment/product/service, please contact me first so that I may          *
 * give you written permission.                                                *
 *                                                                             *
 * This program is distributed without any warranty; without even the          *
 * implied warranty of merchantability or fitness for a particular purpose.    *
 *                                                                             *
 * // End statement                                                            *
 ******************************************************************************/

/* OzSort Version 2.0 Indy Penny Sort. This is the sort-phase (or stage one)  */

#define _FILE_OFFSET_BITS 64
#define _LARGEFILE64_SOURCE

#ifndef _GNU_SOURCE
 #define _GNU_SOURCE
#endif

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <inttypes.h>
#include <assert.h>
#include <pthread.h>
#include <sys/time.h>
#include <aio.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

/* symbolic constants */
#define ALIGNMENT_SIZE 512
#define STACK_SIZE 24
#define TIMER

/* define a 128-bit unsigned integer */
typedef struct ptr_struct
{
  uint64_t key;
  uint64_t payload;
}ptr_struct;

typedef struct timeval timer;

#ifdef TIMER
static timer stage_one_time_start;
static timer stage_one_time_stop;
static timer stage_one_merge_start; 
static timer stage_one_merge_stop;
static timer stage_one_sort_start;
static timer stage_one_sort_stop;
static timer stage_one_disk_start;
static timer stage_one_disk_stop;

static timer stage_one_transfer_start;
static timer stage_one_transfer_stop;

static double stage_one_run_time=0.0;
static double stage_one_merge_time=0.0;
static double stage_one_sort_time=0.0;  
static double stage_one_run_time_total=0.0;
static double stage_one_merge_time_total=0.0;
static double stage_one_sort_time_total=0.0;
static double stage_one_disk_time=0.0;
static double stage_one_disk_time_total=0.0;
static double stage_one_transfer_time=0.0;
static double stage_one_transfer_time_total=0.0;
#endif

/* constant variables used througout the software */
const static uint64_t RECORD_SIZE=100;
const static uint64_t NUM_SORTERS=32;  
const static uint64_t RUN_SIZE=33554432;
const static uint64_t NUM_OUTPUT_BUFFERS=2;
const static uint64_t MICRO_SORT_SIZE=RUN_SIZE/NUM_SORTERS;
const static uint64_t MICRO_SORT_SIZE_BYTES = MICRO_SORT_SIZE * RECORD_SIZE;
const static uint64_t MICRO_SORT_PTR_ARRAY_SIZE = RUN_SIZE * sizeof(struct ptr_struct);
const static uint64_t OUTPUT_BUFFER_SIZE=717440; 
const static uint64_t OUTPUT_BUFFER_SIZE_IN_BYTES=OUTPUT_BUFFER_SIZE*RECORD_SIZE;
const static uint64_t HEAP_ELEMENT_SIZE=24;
const static uint64_t TRANSFER_ELEMENTS=80000;

/* macro's needed for non-recursive qsort */
#define Push(nl,nr) ++stackPtr; stack[stackPtr].l=nl; stack[stackPtr].r=nr;                   
#define Pop(nl,nr) nl=stack[stackPtr].l; nr=stack[stackPtr].r; --stackPtr;
#define Empty (stackPtr<0)

#define swap(x,y) {uint64_t tt; uint64_t tt2;\
   tt=data[x].key;\
   tt2=data[x].payload;\
   data[x].key=data[y].key;\
   data[x].payload=data[y].payload;\
   data[y].key=tt;\
   data[y].payload=tt2;\
   }

/* heap element used in non-recursive qsort */
typedef struct {
   int l,r;
} StackElt;

/* structure used to pass information to sorting threads */
typedef struct thread_data
{
   uint8_t *block;
   ptr_struct *ptrs;
   uint64_t thread_id;
   uint8_t * heap;
   uint8_t **heap_ptrs;
}thread_data;

/* global variables */
static int64_t global_output_file;
static uint64_t k=0;
static uint64_t global_write_offset=0;
static uint64_t thread_complete=0;
static uint8_t * output_buffer;
static uint8_t **ptr_set;

static pthread_t transfer_thread;
static pthread_attr_t attr_detach;
static pthread_mutex_t thread_lock = PTHREAD_MUTEX_INITIALIZER;

static uint8_t * output_buffer_array[NUM_OUTPUT_BUFFERS];
static struct aiocb64 aio_thread[NUM_OUTPUT_BUFFERS];
static struct aiocb64 aio_thread_reserved;

/* returns the first four bytes of a 64-bit integer, which in this case, represent the record offset */
inline static uint32_t get_payload(const register uint64_t p_val)  { return (uint32_t) p_val; }

/* copies a record, word for word, into a buffer, in a manner that helps stimulate out-of-order execution */
inline static void copy_record(uint64_t *dest, const uint64_t *src) 
{
  uint64_t t=0;
  for(; t<12; ++t) dest[t]=src[t];
  *(uint32_t *)(dest+12) = *(uint32_t *)(src+12);
}

/* takes two 128-bit integer structures and compares then lexiographically, in a manner that makes good use of the instruction pipeline */
inline static int64_t key_compare(const register ptr_struct *p1, const register ptr_struct *p2)
{
   if(p1->key < p2->key) return -1;
   if(p1->key > p2->key) return 1;
   if(p1->payload < p2->payload) return -1;
   return 1;
}

/* a thread used to transfer some elements from the start of ptr_set to the output buffer, in an attempt to speed-up the cost of transfer */
static void * transfer_helper(void *ptr)
{
  uint64_t i=0;
  for(; i<TRANSFER_ELEMENTS; ++i)
        copy_record((uint64_t *)( output_buffer + (i * RECORD_SIZE)), (uint64_t *)ptr_set[i]);
}

/* given a block of records read from disk, we iterate though each record and generate a 128-bit integer key for it, relative to the current block */
inline static void gen_key_pointer(ptr_struct *pointer, uint8_t *block, const uint64_t p_count, uint64_t thread_id)
{
    uint8_t *key, *tmp_key1, *tmp_key2;
    uint64_t i=0, j=(thread_id*p_count);
    
    for(; i < p_count; ++i, ++j)
    {
       key =  block  + (RECORD_SIZE*i);
       tmp_key1 = (uint8_t *)&pointer[i].key;
       tmp_key2 = (uint8_t *)&pointer[i].payload;

       *(tmp_key1)  =*(key + 7);
       *(tmp_key1+1)=*(key + 6);
       *(tmp_key1+2)=*(key + 5);
       *(tmp_key1+3)=*(key + 4);
       *(tmp_key1+4)=*(key + 3);
       *(tmp_key1+5)=*(key + 2);
       *(tmp_key1+6)=*(key + 1);
       *(tmp_key1+7)=*(key);

       *(tmp_key2+7)=*(key + 8);
       *(tmp_key2+6)=*(key + 9);
       *(tmp_key2+5)=0;
       *(tmp_key2+4)=0;
       *(uint32_t *)(tmp_key2)=j;
   }
}

/* in-place iterative qsort, tuned to reduced instruction usage while maximizing cache usage. */
static void tuned_qsort(ptr_struct *data, const uint64_t N) 
{
  StackElt stack[STACK_SIZE];
  ptr_struct v;
  const int64_t M=25;
  int64_t l,r,i,j;
  int64_t stackPtr=-1;

  Push(0,N-1);

  while (!Empty) {
    Pop(l,r);

    if ((r-l) <= M) {
      for (i=r-1; i>=l; --i) {
        if (key_compare( &data[i], &data[i+1]) > 0) {    
          v.key=data[i].key; 
	  v.payload=data[i].payload;
          j = i+1;
	  
          do {
            data[j-1].key = data[j].key;
            data[j-1].payload = data[j].payload;
            ++j;
          } while ((j <= r) && (key_compare( &data[j], &v) < 0 )); 

          data[j-1].key = v.key;
          data[j-1].payload = v.payload;
        }
      }
      continue;
    }
    swap((l+r)>>1,l+1);

    if (key_compare( &data[l+1], &data[r]) > 0) swap(l+1,r);
    if (key_compare( &data[l], &data[r]) > 0) swap(l,r);
    if (key_compare( &data[l+1], &data[l]) > 0) swap(l+1,l);
    i = l+1;
    j = r;
   
    v.key = data[l].key;
    v.payload = data[l].payload;

    while(1)
    {
      while (key_compare( &data[++i], &v) < 0);
      while (key_compare( &data[--j], &v) > 0);
      if (j<i) break;
      swap(i,j);

      while (key_compare( &data[++i], &v) < 0);
      while (key_compare( &data[--j], &v) > 0);
      if (j<i) break;
      swap(i,j);

      while (key_compare( &data[++i], &v) < 0);
      while (key_compare( &data[--j], &v) > 0);
      if (j<i) break;
      swap(i,j);

      while (key_compare( &data[++i], &v) < 0);
      while (key_compare( &data[--j], &v) > 0);
      if (j<i) break;
      swap(i,j);

      while (key_compare( &data[++i], &v) < 0);
      while (key_compare( &data[--j], &v) > 0);
      if (j<i) break;
      swap(i,j);

      while (key_compare( &data[++i], &v) < 0);
      while (key_compare( &data[--j], &v) > 0);
      if (j<i) break;
      swap(i,j);

      while (key_compare( &data[++i], &v) < 0);
      while (key_compare( &data[--j], &v) > 0);
      if (j<i) break;
      swap(i,j);

      while (key_compare( &data[++i], &v) < 0);
      while (key_compare( &data[--j], &v) > 0);
      if (j<i) break;
      swap(i,j);
    } 
    swap(l,j);

    if ((j-l) < (r-i+1)) {
      Push(i-1,r);
      Push(l,j); 
    } else {
      Push(l,j);
      Push(i-1,r); 
    }
  }
}

/* binary search designed to find the correct location in a heap of 128-bit key pointers */
inline static int64_t binary_search(uint8_t **heap_ptrs, ptr_struct *current_entry, uint64_t entries)
{
  int64_t mid_pnt=0, left_pnt=0, right_pnt=(entries)-1;
  while(right_pnt >= left_pnt)
  {
    mid_pnt = (left_pnt+right_pnt)>>1;

    if( key_compare(current_entry,  (ptr_struct *)*(heap_ptrs +  mid_pnt)) < 0)
      left_pnt=mid_pnt+1;
    else
      right_pnt=mid_pnt-1;
  }
  return left_pnt;
}

/* store the smallest 128-bit key from a block into a heap containing a sorted list of smallest keys from every block of records */
static void store_in_heap(uint8_t **heap_ptrs, ptr_struct *key, uint64_t microrun_num, uint64_t heap_entries)
{
  ptr_struct *current_entry;
  uint8_t *just_inserted;
  int64_t i=0, j=0;

  current_entry = (ptr_struct *) *(heap_ptrs +  heap_entries);  
  *(uint64_t *)( *(heap_ptrs +  heap_entries) + sizeof(ptr_struct))=microrun_num;
  *current_entry = *key;
  i=binary_search(heap_ptrs, current_entry, heap_entries);

  just_inserted=*(heap_ptrs + heap_entries);
  for(j=(heap_entries)-1; j >= i ; --j) *(heap_ptrs+j+1) = *(heap_ptrs+j);
  *(heap_ptrs+i) = just_inserted;
}

/* store the smallest 128-bit key from a block into an empty heap designed to maintain a sorted list of the smallest keys from each block */
static void store_in_heap_init(uint8_t **heap_ptrs, ptr_struct *key, uint64_t heap_entries)
{
  ptr_struct *current_entry;
  uint8_t *just_inserted;
  int64_t i=0, j=0;

  current_entry = (ptr_struct *) *(heap_ptrs +  heap_entries);;
  just_inserted = *(heap_ptrs + heap_entries);

  *(uint64_t *)( *(heap_ptrs +  heap_entries) + sizeof(ptr_struct))=heap_entries;
  *current_entry = *key;
  i=binary_search(heap_ptrs, current_entry, heap_entries);
  
  for(j=(heap_entries)-1; j >= i ; --j) *(heap_ptrs+j+1) = *(heap_ptrs+j);
  *(heap_ptrs+i) = just_inserted;
}

/* retrieve the first 128-bit key (its bucket ID) from the sorted heap of keys, which will be the smallest key */
inline static uint64_t retrieve_from_heap_stage_two(uint8_t **heap_ptrs, uint64_t heap_entries)
{
  return *(uint64_t *)(  (  (ptr_struct *) *(heap_ptrs+heap_entries)) + 1);
}

/* use the sorted heap of smallest 128-bit keys to merge all buckets together. A bucket or microrun is an even division of a run */
inline static void merge_micro_runs(uint8_t *whole_block, thread_data *thread_data_array, uint8_t *heap, uint8_t **heap_ptrs)
{
  uint32_t curr_count_microrun[NUM_SORTERS];
  uint64_t min_i=0, i=0, heap_entries=NUM_SORTERS, entries_in_output=0, micro_runs_complete=0;  
  memset(curr_count_microrun, 0, sizeof(uint32_t)*NUM_SORTERS);

  while(micro_runs_complete != NUM_SORTERS)
  {
    --heap_entries;
    min_i = retrieve_from_heap_stage_two(heap_ptrs, heap_entries);
    ptr_set[entries_in_output] = whole_block + (get_payload(thread_data_array[min_i].ptrs[curr_count_microrun[min_i]].payload)*RECORD_SIZE);
    
    ++curr_count_microrun[min_i];
    ++entries_in_output;

    if(entries_in_output == OUTPUT_BUFFER_SIZE)
    { 
      #ifdef TIMER
      gettimeofday(&stage_one_transfer_start, NULL);
      #endif
    
      pthread_create(&transfer_thread, NULL, transfer_helper, NULL);
 
      for(i=TRANSFER_ELEMENTS; i<OUTPUT_BUFFER_SIZE; ++i)
        copy_record((uint64_t *)( output_buffer + (i * RECORD_SIZE)), (uint64_t *)ptr_set[i]);
    
      pthread_join(transfer_thread, NULL);
    
      #ifdef TIMER
      gettimeofday(&stage_one_transfer_stop, NULL);
     
      stage_one_transfer_time = 1000.0 * ( stage_one_transfer_stop.tv_sec - stage_one_transfer_start.tv_sec ) + 0.001 * (stage_one_transfer_stop.tv_usec - stage_one_transfer_start.tv_usec );
      stage_one_transfer_time = stage_one_transfer_time/1000.0;
      stage_one_transfer_time_total += stage_one_transfer_time;
      #endif 
    
      aio_thread[k].aio_buf    = output_buffer;
      aio_thread[k].aio_offset = global_write_offset;
      aio_write64( &aio_thread[k] );
      global_write_offset     += (OUTPUT_BUFFER_SIZE_IN_BYTES);
   
      loop_io:
        for(i=0; i<NUM_OUTPUT_BUFFERS; ++i)  if (aio_error64( &aio_thread[i] ) != EINPROGRESS) {k=i; goto bomb_out;}
      goto loop_io;
      bomb_out:

      output_buffer=output_buffer_array[k]; 
      entries_in_output=0; 
    }
    
    if(*(curr_count_microrun+min_i) == MICRO_SORT_SIZE)
    {
      ++micro_runs_complete; continue;
    }
    
    store_in_heap(heap_ptrs, &thread_data_array[min_i].ptrs[curr_count_microrun[min_i]], min_i, heap_entries); 
    ++heap_entries;
  } 
   
  pthread_create(&transfer_thread, NULL, transfer_helper, NULL);
 
  for(i=TRANSFER_ELEMENTS; i<entries_in_output; ++i)
    copy_record((uint64_t *)( output_buffer + (i * RECORD_SIZE)), (uint64_t *)ptr_set[i]);
 
  pthread_join(transfer_thread, NULL);
   
  aio_thread_reserved.aio_buf    = output_buffer;
  aio_thread_reserved.aio_offset = global_write_offset;
  
  aio_write64( &aio_thread_reserved );
  global_write_offset  += (entries_in_output*RECORD_SIZE);
  output_buffer=output_buffer_array[k];

  while(aio_error64(&aio_thread_reserved) == EINPROGRESS); 
}

/* a thread used to sort a micro run */
static void * micro_sorter(void *threadarg)
{
  struct thread_data *my_data = (struct thread_data *) threadarg;
  gen_key_pointer(my_data->ptrs, my_data->block, MICRO_SORT_SIZE, my_data->thread_id);
  tuned_qsort(my_data->ptrs, MICRO_SORT_SIZE);
  *(my_data->heap_ptrs+my_data->thread_id) = (uint8_t *)(my_data->heap+(my_data->thread_id*HEAP_ELEMENT_SIZE));

  pthread_mutex_lock(&thread_lock);
     ++thread_complete; 
  pthread_mutex_unlock(&thread_lock);
}

int main(int argc, char **argv) 
{
  thread_data thread_data_array[NUM_SORTERS];
  pthread_t micro_sorter_thread[NUM_SORTERS];
  int64_t input_file, output_file;
  uint64_t i=0,j=0,file_size=0,num_runs=0;
  uint8_t *block, *heap=NULL, **heap_ptrs=NULL;
  ptr_struct *ptrs;
  struct stat sb;
  
  pthread_attr_init(&attr_detach);
  pthread_attr_setscope(&attr_detach, PTHREAD_SCOPE_PROCESS); 
  pthread_attr_setdetachstate(&attr_detach, PTHREAD_CREATE_DETACHED);
  
  input_file  = open64(argv[1], O_RDONLY | O_NOATIME | O_LARGEFILE | O_DIRECT);
  output_file = open64(argv[1], O_WRONLY | O_CREAT   | O_NOATIME   | O_LARGEFILE | O_DIRECT, 0777);
 
  posix_memalign((void **) &block, ALIGNMENT_SIZE, RUN_SIZE * RECORD_SIZE);
  posix_memalign((void **) &output_buffer, ALIGNMENT_SIZE, OUTPUT_BUFFER_SIZE_IN_BYTES*NUM_OUTPUT_BUFFERS);
  ptrs = (struct ptr_struct *) malloc(MICRO_SORT_PTR_ARRAY_SIZE);
  heap = (uint8_t *)malloc(NUM_SORTERS * HEAP_ELEMENT_SIZE);
  heap_ptrs = (uint8_t **)malloc(NUM_SORTERS * sizeof(uint8_t *));
  ptr_set   = (uint8_t **)calloc(OUTPUT_BUFFER_SIZE, sizeof(uint8_t *));

  stat(argv[1], &sb);
  file_size=(uint64_t)sb.st_size;
  num_runs = file_size / (RUN_SIZE*RECORD_SIZE);
  global_output_file=output_file;
  
  memset(aio_thread, 0, sizeof(struct aiocb64)  * NUM_OUTPUT_BUFFERS);
  memset(&aio_thread_reserved, 0, sizeof(struct aiocb64));
  aio_thread_reserved.aio_fildes = global_output_file;
  aio_thread_reserved.aio_nbytes = (RUN_SIZE - ((RUN_SIZE / (uint64_t)OUTPUT_BUFFER_SIZE) * OUTPUT_BUFFER_SIZE))*RECORD_SIZE;

  for(i=0; i<NUM_OUTPUT_BUFFERS; ++i)
  {
    output_buffer_array[i]=output_buffer + (OUTPUT_BUFFER_SIZE_IN_BYTES*i);
    aio_thread[i].aio_fildes = global_output_file;
    aio_thread[i].aio_nbytes = OUTPUT_BUFFER_SIZE*RECORD_SIZE;
  }
  
  puts("OzSort 2.0 @ Jan 2010. www.ozsort.com\nWritten by Dr. Nikolas Askitis, askitisn@gmail.com");
  puts("-------------------------------------------------");
  printf("RECORD_SIZE=%lu\n", RECORD_SIZE);
  printf("NUM_SORTERS=%lu\n", NUM_SORTERS); 
  printf("RUN_SIZE=%lu\n", RUN_SIZE );
  printf("NUM_OUTPUT_BUFFERS=%lu\n", NUM_OUTPUT_BUFFERS);
  printf("MICRO_SORT_SIZE=%lu\n", MICRO_SORT_SIZE);
  printf("MICRO_SORT_SIZE_BYTES=%lu\n", MICRO_SORT_SIZE_BYTES);
  printf("MICRO_SORT_PTR_ARRAY_SIZE=%lu\n", MICRO_SORT_PTR_ARRAY_SIZE);
  printf("OUTPUT_BUFFER_SIZE=%lu\n", OUTPUT_BUFFER_SIZE);
  printf("OUTPUT_BUFFER_SIZE_IN_BYTES=%lu\n", OUTPUT_BUFFER_SIZE_IN_BYTES);
  printf("HEAP_ELEMENT_SIZE=%lu\n", HEAP_ELEMENT_SIZE);
  printf("STACK_SIZE=%u\n", STACK_SIZE);
  printf("TRANSFER_ELEMENTS=%lu\n", TRANSFER_ELEMENTS);
  #ifdef TIMER
    printf("TIMER ON\n");
  #else
    printf("TIMER OFF\n");
  #endif
  
  printf("\nApprox. mem usage: %.2fMiB\n", 
  ( 
  (RUN_SIZE * RECORD_SIZE) + 16 + 
  (OUTPUT_BUFFER_SIZE_IN_BYTES*NUM_OUTPUT_BUFFERS) + 16 + MICRO_SORT_PTR_ARRAY_SIZE + 16 +
  (NUM_SORTERS * HEAP_ELEMENT_SIZE) + 16 + (NUM_SORTERS * sizeof(uint8_t *)) + 16 + 
  (OUTPUT_BUFFER_SIZE * sizeof(uint8_t *)) + 16 + 8388608 + (sizeof(struct thread_data)*NUM_SORTERS) + 
  (sizeof(pthread_t)*(NUM_SORTERS<<1)))/(double)1048576);
  printf("Number of runs: %lu\n", num_runs);
  puts("Stage 1: generating sorted runs ... ");

  /* initialize the data fields used to communicate to the sorting threads */
  for(i=0; i<NUM_SORTERS; ++i)
  {
      thread_data_array[i].thread_id = i;
      thread_data_array[i].ptrs  = ptrs  + (MICRO_SORT_SIZE*i);
      thread_data_array[i].block = block + (MICRO_SORT_SIZE_BYTES*i);
      thread_data_array[i].heap=heap;
      thread_data_array[i].heap_ptrs=heap_ptrs;
  }

  /* for every run, fetch each of its microruns, one at a time in sequence, spawn a thread to sort it, then wait until all threads finish before merging */
  for(; j<num_runs; ++j)
  {
    #ifdef TIMER
    gettimeofday(&stage_one_time_start, NULL);
    gettimeofday(&stage_one_sort_start, NULL);
    gettimeofday(&stage_one_disk_start, NULL);
    #endif
    
    for(i=0; i<NUM_SORTERS; ++i) 
      read(input_file, thread_data_array[i].block, MICRO_SORT_SIZE_BYTES),
      pthread_create(&micro_sorter_thread[i], &attr_detach, micro_sorter, (void *) &thread_data_array[i]);
    
    #ifdef TIMER
    gettimeofday(&stage_one_disk_stop, NULL);
    stage_one_disk_time = 1000.0 * ( stage_one_disk_stop.tv_sec - stage_one_disk_start.tv_sec ) + 0.001 * (stage_one_disk_stop.tv_usec - stage_one_disk_start.tv_usec );
    stage_one_disk_time = stage_one_disk_time/1000.0;
    stage_one_disk_time_total += stage_one_disk_time;
    #endif 
    
    loop_thread:
      pthread_mutex_lock(&thread_lock);
      if(thread_complete==NUM_SORTERS) {thread_complete=0;  pthread_mutex_unlock(&thread_lock);  goto bomb_out_main;}
      pthread_mutex_unlock(&thread_lock);
    goto loop_thread;
    bomb_out_main:

    for(i=0; i<NUM_SORTERS; ++i) store_in_heap_init(heap_ptrs, &thread_data_array[i].ptrs[0], i);
    
    #ifdef TIMER
    gettimeofday(&stage_one_sort_stop, NULL);
    gettimeofday(&stage_one_merge_start, NULL);
    #endif
    
    merge_micro_runs(block, thread_data_array, heap, heap_ptrs);
   
    #ifdef TIMER
    gettimeofday(&stage_one_merge_stop, NULL);
    gettimeofday(&stage_one_time_stop, NULL);
    
    stage_one_run_time = 1000.0 * ( stage_one_time_stop.tv_sec - stage_one_time_start.tv_sec ) + 0.001 * (stage_one_time_stop.tv_usec - stage_one_time_start.tv_usec );
    stage_one_run_time = stage_one_run_time/1000.0;
    stage_one_run_time_total += stage_one_run_time;

    stage_one_merge_time = 1000.0 * ( stage_one_merge_stop.tv_sec - stage_one_merge_start.tv_sec ) + 0.001 * (stage_one_merge_stop.tv_usec - stage_one_merge_start.tv_usec );
    stage_one_merge_time = stage_one_merge_time/1000.0;
    stage_one_merge_time_total += stage_one_merge_time;
    
    stage_one_sort_time = 1000.0 * ( stage_one_sort_stop.tv_sec - stage_one_sort_start.tv_sec ) + 0.001 * (stage_one_sort_stop.tv_usec - stage_one_sort_start.tv_usec );
    stage_one_sort_time = stage_one_sort_time/1000.0;
    stage_one_sort_time_total += stage_one_sort_time;
   
    printf("Run %lu complete >> read+threadgen: %6.6f  >> keygen+sort: %6.6f  >>  transfer: %6.6f  >> merge+write: %6.6f  >> total: %6.6f\n", j+1, stage_one_disk_time, 
    stage_one_sort_time-stage_one_disk_time, stage_one_transfer_time_total, stage_one_merge_time - stage_one_transfer_time_total, stage_one_run_time); 
    fflush(stdout);
    stage_one_transfer_time_total=0;
    #endif 
  }
  
  #ifdef TIMER
  printf("Total runs %lu  >> avg. read+threadgen: %6.6f  >> avg. keygen+sort: %6.6f  >>  avg. merge+write: %6.6f  >>  avg. total: %6.6f >> expected elapsed time: %6.6f\n", j, 
	 stage_one_disk_time_total/num_runs, (stage_one_sort_time_total - stage_one_disk_time_total)/num_runs, stage_one_merge_time_total/num_runs, stage_one_run_time_total/num_runs,
	 num_runs*(stage_one_run_time_total/num_runs));
  #endif
  
  close(output_file);

  free(block);
  free(ptrs);
  free(heap_ptrs);
  free(heap);
  free(output_buffer_array[0]);
  free(ptr_set);
}
