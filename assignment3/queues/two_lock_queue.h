#include "../common/allocator.h"
#include <mutex>

template <class T>
class Node
{
public:
    T value;
    Node<T> *next;
};

template <class T>
class TwoLockQueue
{
    std::mutex mtx_enqueue;
    std::mutex mtx_dequeue;
    Node<T> *q_head;
    Node<T> *q_tail;

    CustomAllocator my_allocator_;

public:
    TwoLockQueue() : my_allocator_()
    {
        std::cout << "Using TwoLockQueue\n";
    }

    void initQueue(long t_my_allocator_size)
    {
        std::cout << "Using Allocator\n";
        my_allocator_.initialize(t_my_allocator_size, sizeof(Node<T>));
        // Initialize the queue head or tail here
        Node<T> *node = (Node<T> *)my_allocator_.newNode();
        node->next = NULL;
        q_head = node;
        q_tail = node;
        // my_allocator_.freeNode(node);
    }

    void enqueue(T value)
    {
        Node<T> *node = (Node<T> *)my_allocator_.newNode();
        node->value = value;
        node->next = NULL;
        mtx_enqueue.lock();
        q_tail->next = node;
        q_tail = node;
        mtx_enqueue.unlock();
    }

    bool dequeue(T *value)
    {
        bool ret_value = false;
        mtx_dequeue.lock();
        Node<T> *new_head = q_head->next;
        if (new_head == NULL)
        {
            mtx_dequeue.unlock();
            return ret_value;
        }
        *value = new_head->value;
        q_head = new_head;

        mtx_dequeue.unlock();
        // return ret_value;
        return true;
    }

    void cleanup()
    {
        my_allocator_.cleanup();
    }
};