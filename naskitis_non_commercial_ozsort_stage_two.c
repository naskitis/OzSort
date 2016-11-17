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

/* OzSort Version 2.0 Indy Penny Sort. This is the merge-phase (or stage two) */

#define _FILE_OFFSET_BITS 64
#define _LARGEFILE64_SOURCE

#ifndef _GNU_SOURCE 
 #define _GNU_SOURCE 
#endif

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>      
#include <sys/types.h>
#include <unistd.h>
#include <inttypes.h>
#include <assert.h>
#include <pthread.h>
#include <sys/time.h>
#include <aio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h> 

/* symbolic constants */
#define ALIGNMENT_SIZE 512
#define STACK_SIZE 24

/* structure used to define a 128-bit integer key */
typedef struct ptr_struct
{
  uint64_t key;
  uint64_t payload;
}ptr_struct;

typedef struct timeval timer;

#ifdef TIMER
static timer stage_one_merge_start; 
static timer stage_one_merge_stop;
static double stage_one_merge_time=0.0;
static double stage_one_merge_time_total=0.0;
#endif

/* constant global variables used throughout the code */
const static uint64_t RECORD_SIZE=100;
const static uint64_t RUN_DIVIDE_BY=64;  
const static uint64_t RUN_SIZE=33554432;
const static uint64_t RUN_SIZE_IN_BYTES = RUN_SIZE*RECORD_SIZE;
const static uint64_t NUM_OUTPUT_BUFFERS=2;
const static uint64_t MICRO_RUN_SIZE=RUN_SIZE/RUN_DIVIDE_BY;
const static uint64_t MICRO_RUN_SIZE_BYTES = MICRO_RUN_SIZE * RECORD_SIZE;
const static uint64_t OUTPUT_BUFFER_SIZE=717440; 
const static uint64_t OUTPUT_BUFFER_SIZE_IN_BYTES=OUTPUT_BUFFER_SIZE*RECORD_SIZE;
const static uint64_t HEAP_ELEMENT_SIZE=24;
const static uint64_t MAX_RUNS_SUPPORTED=128;

static uint64_t k=0;
static uint64_t global_write_offset=0;
static uint64_t global_file_size=0;

static int64_t global_output_file;
static int64_t global_input_file;
static uint8_t * output_buffer;
static uint8_t * output_buffer_array[NUM_OUTPUT_BUFFERS];

static struct aiocb64 aio_thread[NUM_OUTPUT_BUFFERS];
static struct aiocb64 aio_thread_reserved;
static struct aiocb64 aio_reader;

/* prints a record to the screen */
static void print_record(uint8_t *tmp)
{
  register uint64_t i;
  for(i=0; i<RECORD_SIZE; i++)
  printf("%c", *(tmp+i));
  fflush(stdout);
}

/* copies a record to an output buffer, in a manner than helps promote out-of-order execution */
inline static void copy_record(uint64_t *dest, const uint64_t *src) 
{
  register uint64_t t=0;
  for(; t<12; ++t) dest[t]=src[t];
  *(uint32_t *)(dest+12) = *(uint32_t *)(src+12);
}

/* compares two 128-bit integer values using integer arithmetic */ 
inline static int64_t key_compare(const register ptr_struct *p1, const register ptr_struct *p2)
{
   if(p1->key < p2->key) return -1;
   if(p1->key > p2->key) return 1;
   if(p1->payload < p2->payload) return -1;
   return 1;
}

/* transfers a full output buffer to disk, and when possible, immediately returns to the caller with the next empty output buffer */
inline static void copy_into_output_buffer(uint8_t *string, uint64_t *entries_in_output)
{ 
  copy_record((uint64_t *)( output_buffer + (*entries_in_output * RECORD_SIZE)), (uint64_t *)string);
  *entries_in_output = *entries_in_output + 1;

  if(*entries_in_output == OUTPUT_BUFFER_SIZE)
  { 
    aio_thread[k].aio_buf    = output_buffer;
    aio_thread[k].aio_offset = global_write_offset;
    aio_write64( &aio_thread[k] );
    global_write_offset     += (OUTPUT_BUFFER_SIZE_IN_BYTES);

    register uint64_t i=0;
    loop_io:
      for(i=0; i<NUM_OUTPUT_BUFFERS; ++i)  if (aio_error64(&aio_thread[i]) != EINPROGRESS) {k=i; goto bomb_out;}
    goto loop_io;
    bomb_out:

    output_buffer=output_buffer_array[k];  
    *entries_in_output=0; 
    printf("%.1f%%   \r", (global_write_offset/(double)global_file_size)*100.0);
    fflush(stdout);
  }
}

/* generates an 128-bit integer representation of a record */
inline static void gen_key_pointer(ptr_struct *pointer, uint8_t *block)
{
    uint8_t *tmp_key1, *tmp_key2;

    tmp_key1 = (uint8_t *)&pointer->key;
    *(tmp_key1)  =*(block + 7);
    *(tmp_key1+1)=*(block + 6);
    *(tmp_key1+2)=*(block + 5);
    *(tmp_key1+3)=*(block + 4);
    *(tmp_key1+4)=*(block + 3);
    *(tmp_key1+5)=*(block + 2);
    *(tmp_key1+6)=*(block + 1);
    *(tmp_key1+7)=*(block);

    tmp_key2 = (uint8_t *)&pointer->payload;
    *(tmp_key2+7)=*(block + 8);
    *(tmp_key2+6)=*(block + 9);
    *(tmp_key2+5)=0;
    *(tmp_key2+4)=0;
    *(tmp_key2+3)=0;
    *(tmp_key2+2)=0;
    *(tmp_key2+1)=0;
    *(tmp_key2)=0;
}

/* conducts a binary search on the sorted 128-bit keys in the heap, to find the correct location of the current_entry */
inline static int64_t binary_search(uint8_t **heap_ptrs, ptr_struct *current_entry, const uint64_t entries)
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

/* stores a 128-bit key into a sorted heap, where the smallest key appears first */
static void store_in_heap(uint8_t **heap_ptrs, ptr_struct *key, uint64_t microrun_num, const uint64_t heap_entries)
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

/* stores a 128bit key into an initially empty heap. The reason we have a separate init function is because not all heap pointers have been initialized yet */
static void store_in_heap_init(uint8_t **heap_ptrs, ptr_struct *key, const uint64_t heap_entries)
{
  ptr_struct *current_entry;
  uint8_t *just_inserted;
  int64_t i=0, j=0;

  current_entry = (ptr_struct *) *(heap_ptrs +  heap_entries);
  just_inserted = *(heap_ptrs + heap_entries);

  *(uint64_t *)( *(heap_ptrs +  heap_entries) + sizeof(ptr_struct))=heap_entries;
  *current_entry = *key;
  i=binary_search(heap_ptrs, current_entry, heap_entries);
  
  for(j=(heap_entries)-1; j >= i ; --j) *(heap_ptrs+j+1) = *(heap_ptrs+j);
  *(heap_ptrs+i) = just_inserted;
}

/* retrieve the smallest key from the sorted heap, which represents the bucket ID containing the smallest key in the current run */
inline static uint64_t retrieve_from_heap_stage_two(uint8_t **heap_ptrs, const uint64_t heap_entries)
{
  return *(uint64_t *)(  (  (ptr_struct *) *(heap_ptrs+heap_entries)) + 1);
}

/* merge all microruns together using the heap */
inline static void merge_micro_runs(uint8_t *whole_block, uint8_t *heap, uint8_t **heap_ptrs, uint32_t *num_microruns_per_run, const uint32_t p_runs)
{
  uint64_t num_seeks=0, min_i=0, heap_entries=p_runs, entries_in_output=0, runs_complete=0;
  uint8_t  *min_string=NULL, *new_string=NULL;
  uint32_t curr_count_microrun[MAX_RUNS_SUPPORTED]; 
  ptr_struct tmp_key;
  
  memset(curr_count_microrun, 0, sizeof(uint32_t)*p_runs);
  memset(&tmp_key, 0, sizeof(struct ptr_struct));

  while(runs_complete != p_runs)
  {
    --heap_entries;
    min_i = retrieve_from_heap_stage_two(heap_ptrs, heap_entries);
    min_string = whole_block + curr_count_microrun[min_i] + (min_i*MICRO_RUN_SIZE_BYTES);
    
    curr_count_microrun[min_i]+=RECORD_SIZE;
    copy_into_output_buffer(min_string, &entries_in_output);

    if(*(curr_count_microrun+min_i) == MICRO_RUN_SIZE_BYTES)
    {
      ++num_microruns_per_run[min_i];
      
      if(num_microruns_per_run[min_i] == RUN_DIVIDE_BY)
      {
        ++runs_complete; continue;
      }
      
      aio_reader.aio_buf    = whole_block + (min_i*MICRO_RUN_SIZE_BYTES);
      aio_reader.aio_offset = (min_i*RUN_SIZE_IN_BYTES)+(MICRO_RUN_SIZE_BYTES*num_microruns_per_run[min_i]);
      aio_read64( &aio_reader );
      
      curr_count_microrun[min_i]=0;
      ++num_seeks;
      while(aio_error64(&aio_reader) == EINPROGRESS); 
    }
    new_string  = whole_block + curr_count_microrun[min_i] + (min_i*MICRO_RUN_SIZE_BYTES);
    gen_key_pointer(&tmp_key, new_string);
    store_in_heap(heap_ptrs, &tmp_key, min_i, heap_entries); 
    ++heap_entries;
  }
  aio_thread_reserved.aio_fildes = global_output_file;
  aio_thread_reserved.aio_buf    = output_buffer;
  aio_thread_reserved.aio_offset = global_write_offset;
  aio_thread_reserved.aio_nbytes = entries_in_output*RECORD_SIZE;
  
  aio_write64( &aio_thread_reserved );
  global_write_offset  += (entries_in_output*RECORD_SIZE);
  output_buffer=output_buffer_array[k];
 
  while(aio_error64(&aio_thread_reserved) == EINPROGRESS); 
  printf("Merge completed %lu seeks >> %6.3f MB/seek >> %u runs\n",  num_seeks, (MICRO_RUN_SIZE_BYTES/(double)1000000),  p_runs); 
}

/* initialize the stage two merging procedure by first reading in the first microrun from each run in an async manner while populating the heap */
static void stage_two(const uint32_t p_runs) 
{
    uint8_t  *heap=NULL,**heap_ptrs=NULL,*tmp=NULL,*tmp_curr=NULL,*microrun_workingset;
    uint32_t num_microruns_per_run[MAX_RUNS_SUPPORTED]; 
    ptr_struct tmp_key;
  
    /* allocate the required memory to store the first mircoruns from each run, along with the heap and its pointers */
    assert((heap = (uint8_t *)calloc(p_runs * HEAP_ELEMENT_SIZE, sizeof(uint8_t)))!=NULL);
    assert((heap_ptrs = (uint8_t **)calloc(p_runs, sizeof(uint8_t *))) != NULL);
    assert(posix_memalign((void **) &microrun_workingset, ALIGNMENT_SIZE, p_runs * MICRO_RUN_SIZE_BYTES)==0);
    tmp=tmp_curr=microrun_workingset;
    
    bzero(&tmp_key, sizeof(struct ptr_struct));
    bzero(&aio_reader, sizeof(struct aiocb64));
    bzero(&num_microruns_per_run, sizeof(uint32_t)*p_runs);

    aio_reader.aio_fildes = global_input_file;
    aio_reader.aio_nbytes = MICRO_RUN_SIZE_BYTES;
    aio_reader.aio_buf    = tmp;
    aio_reader.aio_offset = 0;
    
    /* read in the first mirco run from the first run */
    aio_read64( &aio_reader );
    while(aio_error64(&aio_reader) == EINPROGRESS); 

    *(heap_ptrs+0) = (uint8_t *)heap;
    tmp=tmp_curr;
    
    /* grab the next microrun from disk while we process the current microrun */
    register uint64_t i=1;
    for(i=1; i<p_runs; ++i) 
    {
      tmp_curr = (uint8_t *)(microrun_workingset + (i * MICRO_RUN_SIZE_BYTES)); 
       
      aio_reader.aio_buf    = tmp_curr;
      aio_reader.aio_offset = (i * RUN_SIZE_IN_BYTES);
      aio_read64( &aio_reader );

      printf("%4ld  ", i-1);
      print_record(tmp);
      
      *(heap_ptrs+i) = (uint8_t *)(heap+(i*HEAP_ELEMENT_SIZE));
      gen_key_pointer(&tmp_key, tmp);
      store_in_heap_init(heap_ptrs, &tmp_key, i-1);

      while(aio_error64(&aio_reader) == EINPROGRESS);
      tmp=tmp_curr;
    }
    *(heap_ptrs+i-1) = (uint8_t *)(heap+((i-1)*HEAP_ELEMENT_SIZE));
    gen_key_pointer(&tmp_key, tmp);
    store_in_heap_init(heap_ptrs, &tmp_key, i-1);

    printf("%4ld  ", i-1);
    print_record(tmp);
     
    /* begin merging the micro runs from disk */
    merge_micro_runs(microrun_workingset, heap, heap_ptrs, num_microruns_per_run, p_runs);

    free(heap); 
    free(heap_ptrs);
    free(microrun_workingset);
}

int main(int argc, char **argv) 
{
  int64_t input_file, output_file;
  uint64_t i=0,file_size=0,num_runs=0;
  struct stat sb;
  
  unlink(argv[2]); 
  
  input_file  = open64(argv[1], O_RDONLY | O_NOATIME | O_LARGEFILE | O_DIRECT);
  output_file = open64(argv[2], O_WRONLY | O_CREAT   | O_NOATIME   | O_LARGEFILE | O_DIRECT, 0777);
 
  posix_memalign((void **) &output_buffer, ALIGNMENT_SIZE, OUTPUT_BUFFER_SIZE_IN_BYTES*NUM_OUTPUT_BUFFERS);

  stat(argv[1], &sb);
  file_size=(uint64_t)sb.st_size;
  num_runs = file_size / (RUN_SIZE*RECORD_SIZE);
  global_output_file=output_file;
  global_input_file=input_file;
  global_file_size=file_size;
   
  bzero(aio_thread, sizeof(struct aiocb64)  * NUM_OUTPUT_BUFFERS);
  bzero(&aio_thread_reserved, sizeof(struct aiocb64));

  for(i=0; i<NUM_OUTPUT_BUFFERS; ++i)
  {
    output_buffer_array[i]=output_buffer + (OUTPUT_BUFFER_SIZE_IN_BYTES*i);
    aio_thread[i].aio_fildes = global_output_file; aio_thread[i].aio_nbytes = OUTPUT_BUFFER_SIZE*RECORD_SIZE;
  }
  
  puts("OzSort 2.0 @ Jan 2010. www.ozsort.com\nWritten by Dr. Nikolas Askitis, askitisn@gmail.com");
  puts("Stage two --- merger");
  puts("-------------------------------------------------");
  printf("RECORD_SIZE=%lu\n", RECORD_SIZE);
  printf("RUN_DIVIDE_BY=%lu\n", RUN_DIVIDE_BY); 
  printf("RUN_SIZE=%lu\n", RUN_SIZE );
  printf("NUM_OUTPUT_BUFFERS=%lu\n", NUM_OUTPUT_BUFFERS);
  printf("MICRO_RUN_SIZE=%lu\n", MICRO_RUN_SIZE);
  printf("MICRO_RUN_SIZE_BYTES=%lu\n", MICRO_RUN_SIZE_BYTES);
  printf("OUTPUT_BUFFER_SIZE=%lu\n", OUTPUT_BUFFER_SIZE);
  printf("OUTPUT_BUFFER_SIZE_IN_BYTES=%lu\n", OUTPUT_BUFFER_SIZE_IN_BYTES);
  printf("HEAP_ELEMENT_SIZE=%lu\n", HEAP_ELEMENT_SIZE);
  printf("STACK_SIZE=%u\n", STACK_SIZE);
  printf("MAX_RUNS_SUPPORTED=%lu >> %6.2fGB max (configurable)\n", MAX_RUNS_SUPPORTED, (RUN_SIZE_IN_BYTES *MAX_RUNS_SUPPORTED)/(double)1000000000 );
  
  #ifdef TIMER
    printf("TIMER ON\n");
  #else
    printf("TIMER OFF\n");
  #endif
  
  stage_two(num_runs);
  
  free(output_buffer_array[0]);
  close(output_file);
  close(input_file);
}
