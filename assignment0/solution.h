#include "core/circular_queue.h"
#include "core/utils.h"
#include <pthread.h>
#include <stdlib.h>
#include <mutex>
class ProducerConsumerProblem;

class Producer
{
public:
  int producer_id;
  long item_produced;
  long value_produced;
  CircularQueue *production_buff;
  double time_taken_prod;
  ProducerConsumerProblem *producer;
};

class Consumer
{
public:
  int consumer_id;
  long item_consumed;
  long value_consumed;
  CircularQueue *production_buff;
  double time_taken_cons;
  ProducerConsumerProblem *consumer;
};

class ProducerConsumerProblem
{
public:
  long n_items;
  int n_producers;
  int n_consumers;
  CircularQueue production_buffer;

  Producer *producers;
  Consumer *consumers;

  // Dynamic array of thread identifiers for producer and consumer threads.
  // Use these identifiers while creating the threads and joining the threads.
  pthread_t *producer_threads;
  pthread_t *consumer_threads;

  int active_producer_count;
  int active_consumer_count;

  pthread_mutex_t mutex;
  pthread_cond_t empty_condition;
  pthread_cond_t full_condition;

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
