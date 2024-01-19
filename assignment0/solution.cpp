#include "solution.h"

void *producerFunction(void *_arg)
{
  // Parse the _arg passed to the function.
  // Enqueue `n` items into the `production_buffer`. The items produced should
  // be 0, 1, 2,..., (n-1).
  // Keep track of the number of items produced and the value produced by the
  // thread.
  // The producer that was last active should ensure that all the consumers have
  // finished. NOTE: Each thread will enqueue `n` items.
  // Use mutex variables and conditional variables as necessary.
  // Each producer enqueues `n` items

  timer timer1;
  timer1.start();

  long item = 0;
  long items_produced = 0;
  long value_produced = 0;

  Producer *basic_producers = (Producer *)_arg;
  ProducerConsumerProblem *problem = basic_producers->producer;
  long n_produces = problem->n_items;
  // CircularQueue production_buffer(problem->production_buffer);
  int prod_flag = 0;

  while (items_produced < n_produces)
  {
    pthread_mutex_lock(&problem->mutex);
    bool ret = problem->production_buffer.enqueue(item);
    if (ret == true)
    {
      if (problem->production_buffer.itemCount() == 1)
      {
        // The queue is no longer empty
        // Signal all consumers indicating queue is not empty
        pthread_cond_broadcast(&problem->empty_condition);
      }
      pthread_mutex_unlock(&problem->mutex);
      value_produced += item;
      items_produced++;
      item++;
    }
    else
    {
      // production_buffer is full, so block on conditional variable waiting for consumer to signal.
      // pthread_cond_broadcast(&problem->full_condition);
      if (problem->production_buffer.isFull())
      {
        pthread_cond_wait(&problem->full_condition, &problem->mutex);
        pthread_mutex_unlock(&problem->mutex);
      }
    }
  }

  pthread_mutex_lock(&problem->mutex);
  basic_producers->item_produced = items_produced;
  basic_producers->value_produced = value_produced;
  // After production is completed:
  // Update the number of producers that are currently active.
  --problem->active_producer_count;
  // --active_producer_count;
  pthread_mutex_unlock(&problem->mutex);
  // The producer that was last active (can be determined using `active_producer_count`) will keep signalling the consumers until all consumers have finished (can be determined using `active_consumer_count`).

  if (problem->active_producer_count == 0)
  {
    prod_flag = 1;
  }

  if (prod_flag)
  {
    while (problem->active_consumer_count > 0)
    {
      pthread_cond_broadcast(&problem->empty_condition);
    }
  }
  basic_producers->time_taken_prod = timer1.stop();
  pthread_exit(basic_producers);
}

void *consumerFunction(void *_arg)
{
  // Parse the _arg passed to the function.
  // The consumer thread will consume items by dequeueing the items from the
  // `production_buffer`.
  // Keep track of the number of items consumed and the value consumed by the
  // thread.
  // Once the productions is complete and the queue is also empty, the thread
  // will exit. NOTE: The number of items consumed by each thread need not be
  // same.
  // Use mutex variables and conditional variables as necessary.
  // Each consumer dequeues items from the `production_buffer`

  timer timer2;
  timer2.start();
  long item;
  long items_consumed = 0;
  long value_consumed = 0;

  Consumer *basic_consumers = (Consumer *)_arg;
  ProducerConsumerProblem *problem = basic_consumers->consumer;
  long n_consumes = problem->n_items;
  // CircularQueue production_buffer = problem->production_buffer;

  while (true)
  {
    pthread_mutex_lock(&problem->mutex);
    bool ret = problem->production_buffer.dequeue(&item);
    if (ret == true)
    {
      if (problem->production_buffer.itemCount() == problem->production_buffer.getCapacity() - 1)
      {
        // The queue is no longer full
        // Signal all producers indicating queue is not full
        pthread_cond_broadcast(&problem->full_condition);
      }
      pthread_mutex_unlock(&problem->mutex);
      value_consumed += item;
      items_consumed++;
    }
    else
    {
      // production_buffer is empty, so block on conditional variable waiting for producer to signal.
      if (problem->production_buffer.isEmpty())
      {
        // The thread can wake up because of 2 scenarios:
        // Scenario 1: There are no more active producers (i.e., production is complete) and the queue is empty. This is the exit condition for consumers, and at this point consumers should decrement `active_consumer_count`.
        if (problem->active_producer_count == 0)
        {
          pthread_mutex_unlock(&problem->mutex);
          break;
        }
        // Scenario 2: The queue is not empty and/or the producers are active. Continue consuming.
        else
        {
          pthread_cond_wait(&problem->empty_condition, &problem->mutex);
          pthread_mutex_unlock(&problem->mutex);
        }
      }
      // pthread_mutex_unlock(&problem->mutex);
    }
  }
  pthread_mutex_lock(&problem->mutex);
  --problem->active_consumer_count;
  basic_consumers->item_consumed = items_consumed;
  basic_consumers->value_consumed = value_consumed;
  pthread_mutex_unlock(&problem->mutex);

  basic_consumers->time_taken_cons = timer2.stop();
  pthread_exit(basic_consumers);
}

ProducerConsumerProblem::ProducerConsumerProblem(long _n_items,
                                                 int _n_producers,
                                                 int _n_consumers,
                                                 long _queue_size)
    : n_items(_n_items), n_producers(_n_producers), n_consumers(_n_consumers),
      production_buffer(_queue_size)
{
  std::cout << "Constructor\n";
  std::cout << "Number of items: " << n_items << "\n";
  std::cout << "Number of producers: " << n_producers << "\n";
  std::cout << "Number of consumers: " << n_consumers << "\n";
  std::cout << "Queue size: " << _queue_size << "\n";

  producer_threads = new pthread_t[n_producers];
  consumer_threads = new pthread_t[n_consumers];
  if (n_producers > 0)
  {
    producers = new Producer[n_producers];
  }

  if (n_consumers > 0)
  {
    consumers = new Consumer[n_consumers];
  }

  // Initialize all mutex and conditional variables here.
  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&full_condition, NULL);
  pthread_cond_init(&empty_condition, NULL);
}

ProducerConsumerProblem::~ProducerConsumerProblem()
{
  std::cout << "Destructor\n";
  delete[] producer_threads;
  delete[] consumer_threads;
  delete[] producers;
  delete[] consumers;
  // Destroy all mutex and conditional variables here.
  pthread_mutex_destroy(&mutex);
  pthread_cond_destroy(&full_condition);
  pthread_cond_destroy(&empty_condition);
}

void ProducerConsumerProblem::startProducers()
{
  std::cout << "Starting Producers\n";
  active_producer_count = n_producers;
  // Create producer threads P1, P2, P3,.. using pthread_create.
  if (producers)
  {
    for (int i = 0; i < n_producers; i++)
    {
      producers[i].producer_id = i;
      producers[i].producer = this;
      pthread_create(&producer_threads[i], NULL, producerFunction, &producers[i]);
    }
  }
  // std::cout << "finishing Producers\n";
}

void ProducerConsumerProblem::startConsumers()
{
  std::cout << "Starting Consumers\n";
  active_consumer_count = n_consumers;
  // Create consumer threads C1, C2, C3,.. using pthread_create.
  if (consumers)
  {
    for (int i = 0; i < n_consumers; i++)
    {
      consumers[i].consumer_id = i;
      consumers[i].consumer = this;
      pthread_create(&consumer_threads[i], NULL, consumerFunction, &consumers[i]);
    }
  }
  // std::cout << "finishing Consumers\n";
}

void ProducerConsumerProblem::joinProducers()
{
  std::cout << "Joining Producers\n";
  // Join the producer threads with the main thread using pthread_join
  // std::cout << "n_producers is : " << n_producers << '\n';
  for (int i = 0; i < n_producers; i++)
  {
    // std::cout << "0000000000\n";
    pthread_join(producer_threads[i], NULL);
    // std::cout << "111111111111111\n";
  }
  // std::cout << "finishing Producers threasd\n";
}

void ProducerConsumerProblem::joinConsumers()
{
  std::cout << "Joining Consumers\n";
  // Join the consumer threads with the main thread using pthread_join
  for (int i = 0; i < n_consumers; i++)
  {
    pthread_join(consumer_threads[i], NULL);
  }
  // std::cout << "finishing Consumers threasd\n";
}

void ProducerConsumerProblem::printStats()
{
  std::cout << "Producer stats\n";
  std::cout << "producer_id, items_produced, value_produced, time_taken \n";
  // Make sure you print the producer stats in the following manner
  // 0, 1000, 499500, 0.00123596
  // 1, 1000, 499500, 0.00154686
  // 2, 1000, 499500, 0.00122881
  long total_produced = 0;
  long total_value_produced = 0;
  for (int i = 0; i < n_producers; i++)
  {
    std::cout << producers[i].producer_id << ", " << producers[i].item_produced << ", " << producers[i].value_produced << ", " << producers[i].time_taken_prod << std::endl;
    total_produced += producers[i].item_produced;
    total_value_produced += producers[i].value_produced;
  }
  std::cout << "Total produced = " << total_produced << "\n";
  std::cout << "Total value produced = " << total_value_produced << "\n";
  std::cout << "Consumer stats\n";
  std::cout << "consumer_id, items_consumed, value_consumed, time_taken \n";
  // Make sure you print the consumer stats in the following manner
  // 0, 677, 302674, 0.00147414
  // 1, 648, 323301, 0.00142694
  // 2, 866, 493382, 0.00139689
  // 3, 809, 379143, 0.00134516
  long total_consumed = 0;
  long total_value_consumed = 0;
  for (int i = 0; i < n_consumers; i++)
  {
    std::cout << consumers[i].consumer_id << ", " << consumers[i].item_consumed << ", " << consumers[i].value_consumed << ", " << consumers[i].time_taken_cons << std::endl;
    total_consumed += consumers[i].item_consumed;
    total_value_consumed += consumers[i].value_consumed;
  }
  std::cout << "Total consumed = " << total_consumed << "\n";
  std::cout << "Total value consumed = " << total_value_consumed << "\n";
}