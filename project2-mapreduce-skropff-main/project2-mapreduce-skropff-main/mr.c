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
  // printf("beginning\n");
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
    // printf("beginning loop\n");
    if (current != NULL) {
      printf("current string: %s\n", (get_kv(current))->key);
    }
    if (string == NULL || (strcmp((get_kv(current))->key, string) != 0)) {
      // printf("A\n");
      if (string != NULL) {
        /*
        printf("string: %s\n", string);
        printf("array[count] is NULL: %d\n", array[count] == NULL);
        printf("arg1->output is NULL: %d\n", arg1->output == NULL);
        */
        (arg1->reducer)(string, array[count], arg1->output);
      }
      count = count + 1;
      // printf("ending A\n");
      if (count > size) {
        array = (kvlist_t **) realloc(array, 2 * size * sizeof(kvlist_t *));
        size = size * 2;
      }
      array[count] = kvlist_new();
      string = (get_kv(current))->key;
      kvlist_append(array[count], get_kv(current));
    }
    else {
      // printf("B\n");
      kvlist_append(array[count], get_kv(current));
    }
    // printf("go\n");
    current = get_next(current);
    // printf("ending loop\n");
  }
  if (string != NULL) {
    (arg1->reducer)(string, array[count], arg1->output);
  }
  // printf("ending\n");
  return NULL;
}

void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,
                size_t num_reducer, kvlist_t* input, kvlist_t* output) {
  //Split phase
  kvlist_t *lists[num_mapper];
  kvlist_t *lists2[num_mapper];
  kvlist_node_t *current;
  pthread_t mapper_id[num_mapper];
  map_group input1;
  reduce_group input2;
  for (int i = 0; i < (int) num_mapper; i = i + 1) {
    lists[i] = kvlist_new();
    lists2[i] = kvlist_new();
  }
  current = get_head(input);
  for (int i = 0;; i = i + 1) {
    if (current == NULL) {
      break;
    }
    else {
      printf("appending\n");
      kvlist_append(lists[i % num_mapper], get_kv(current));
      current = get_next(current);
    }
  }
  // printf("Bench\n");
  //Transititon from split to map phase
    /*
  for (int i = 0; i < (int) num_mapper; i = i + 1) {
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
  for (int i = 0; i < (int) num_mapper; i = i + 1) {
    input1.mapper = mapper;
    input1.input = lists[i];
    input1.output = lists2[i];
    pthread_create(mapper_id + i, NULL, (void *(*)(void *)) &mapper_prepare, (void *) (&input1));
  }
  for (int i = 0; i < (int) num_mapper; i = i + 1) {
    pthread_join(mapper_id[i], NULL);
  }
  // printf("Bench2\n");
  //Shuffle phase
  kvlist_t *lists3[num_reducer];
  kvlist_t *lists4[num_reducer];
  for (int i = 0; i < (int) num_reducer; i = i + 1) {
    lists3[i] = kvlist_new();
    lists4[i] = kvlist_new();
  }
  for (int i = 0; i < (int) num_mapper; i = i + 1) {
    current = get_head(lists2[i]);
    while (current != NULL) {
      kvlist_append(lists3[(hash(get_kv(current)->key)) % num_reducer], get_kv(current));
      current = get_next(current);
    }
  }
  // printf("Bench3\n");
  //Reduce phase
  pthread_t reducer_id[num_reducer];
  for (int i = 0; i < (int) num_reducer; i = i + 1) {
    input2.reducer = reducer;
    input2.lst = lists3[i];
    input2.output = lists4[i];
    pthread_create(reducer_id + i, NULL, (void *(*)(void *)) &reducer_prepare, (void *) &input2);
  }
  // printf("Bench4\n");
  for (int i = 0; i < (int) num_reducer; i = i + 1) {
    pthread_join(reducer_id[i], NULL);
  }
  for (int i = 0; i < (int) num_reducer; i = i + 1) {
    kvlist_extend(output, lists4[i]);
  }
}
