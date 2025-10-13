#include "mr.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hash.h"
#include "kvlist.h"

typedef struct {
  mapper_t mapper;
  kvlist_t *input;
  kvlist_t *output;
} map_group;

typedef struct {
  reducer_t reducer;
  kvlist_t *lst;
  kvlist_t *output;
} reduce_group;

void *mapper_prepare(void *arg);

void *mapper_prepare(void *arg) {
  map_group *arg1;
  arg1 = (map_group *) arg;
  kvlist_node_t *current;
  current = get_head(arg1->input);
  // start = current;
  while (current != NULL) {
      // start->value = (char *) realloc(start->value, strlen(statt->value) + strlen(current->value) + 2);
      // strcat(start->value, space);
      // strcat(start->value, current->value);
      (arg1->mapper)(get_kv(current), arg1->output);
      current = get_next(current);
    }
  return NULL;
}

void *reducer_prepare(void *arg);

void *reducer_prepare(void *arg) {
  reduce_group *arg1;
  arg1 = (reduce_group *) arg;
  kvlist_sort(arg1->lst);
  //Create a realloc() array of linked list pointers. 
  kvlist_t **array;
  array = (kvlist_t **) malloc(sizeof(kvlist_t *));
  array[0] = kvlist_new();
  int count;
  count = 0;
  int size;
  size = 1;
  kvlist_node_t *current;
  current = get_head(arg1->lst); 
  char *string;
  string = NULL;
  while (current != NULL) {
    if (string == NULL || (strcmp((get_kv(current))->key, string) != 0) {
      if (string != NULL) {
        (arg1->reducer)(string, array[current], arg->output);
      }
      count = count + 1;
      if (count > size) {
        array = (kvlist **) realloc(array, 2 * size);
        size = size * 2;
      }
      array[count] = kvlist_new();
      string = (current->kv)->key;
      kvlist_append(array[count], current);
    }
    else {
      kvlist_append(array[count], current);
    }
    current = current->next;
  }
  return NULL;
}

void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,
                size_t num_reducer, kvlist_t* input, kvlist_t* output) {
  //Split phase
  kvlist_t *lists[num_mapper];
  kvlist_t *lists2[num_mapper];
  kvpair_t *current;
  kvpair_t *start;
  pthread_id mapper_id[num_mapper];
  char space[] = " ";
  map_group input1;
  reduce_group input2;
  for (int i = 0; i < num_mapper; i = i + 1) {
    lists[i] = kvlist_new();
    lists2[i] = kvlist_new();
  current = input->head;
  for (int i = 0;; i = i + 1) {
    if (current == NULL) {
      break;
    }
    else {
      kvlist_append(lists[i % num_mapper], current);
      current = current->next;
    }
  }
  //Transititon from split to map phase
    /*
  for (int i = 0; i < num_mapper; i = i + 1) {
    current = (lists[i])->head;
    start = current;
    while (current != NULL) {
      if (current != start) {
        start->value = (char *) realloc(start->value, strlen(statt->value) + strlen(current->value) + 2);
        strcat(start->value, space);
        strcat(start->value, current->value);
      }
      current = current->next;
    }
  }
    */
    
  //Map phase
  for (int i = 0; i < num_mapper; i = i + 1) {
    input1.mapper = mapper;
    input1.input = lists[i];
    input1.output = lists2[i];
    pthread_create(mapper_id + i, NULL, (void *) &mapper_prepare, (void *) (&input1));
  }
  for (int i = 0; i < num_mapper; i = i + 1) {
    pthread_join(mapper_id + i, NULL);
  }
  //Shuffle phase
  kvlist_t *lists3[num_reducer];
  kvlist_t *lists4[num_reducer];
  for (int i = 0; i < num_reducer; i = i + 1) {
    lists3[i] = kvlist_new();
  }
  for (int i = 0; i < num_mapper; i = i + 1) {
    current = (lists2[i])->head;
    while (current == NULL) {
      kvlist_append(lists3[(hash(current->key)) % num_reducer], current);
      current = current->next;
    }
  }
  //Reduce phase
  pthread_id reducer_id[num_producer];
  for (int i = 0; i < num_producer; i = i + 1) {
    input2->reducer = reducer;
    input2->lst = lists3[i];
    input2->output = lists4[i];
    pthread_create(reducer_id + i, NULL, &reducer_prepare, (void *) input2);
  }
  for (int i = 0; i < num_reducer; i = i + 1) {
    kvlist_extend(output, lists4[i]);
  }
}
