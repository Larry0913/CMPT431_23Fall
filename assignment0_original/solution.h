#include "core/circular_queue.h"
#include "core/utils.h"
#include <pthread.h>
#include <stdlib.h>

class ProducerConsumerProblem {
  long n_items;
  int n_producers;
  int n_consumers;
  CircularQueue production_buffer;

  // Dynamic array of thread identifiers for producer and consumer threads.
  // Use these identifiers while creating the threads and joining the threads.
  pthread_t *producer_threads;
  pthread_t *consumer_threads;

  int active_producer_count;
  int active_consumer_count;

public:
  // The following 6 methods should be defined in the implementation file (solution.cpp)
  ProducerConsumerProblem(long _n_items, int _n_producers, int _n_consumers,
                          long _queue_size);
  ~ProducerConsumerProblem();
  void startProducers();
  void startConsumers();
  void joinProducers();
  void joinConsumers();
  void printStats();
};
