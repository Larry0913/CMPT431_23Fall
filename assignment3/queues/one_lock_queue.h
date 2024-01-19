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
class OneLockQueue
{
    Node<T> *q_head;
    Node<T> *q_tail;
    std::mutex mtx;
    CustomAllocator my_allocator_;

public:
    OneLockQueue() : my_allocator_()
    {
        std::cout << "Using OneLockQueue\n";
    }

    void initQueue(long t_my_allocator_size)
    {
        std::cout << "Using Allocator\n";
        my_allocator_.initialize(t_my_allocator_size, sizeof(Node<T>));
        // Initialize the queue head or tail here
        Node<T> *newNode = (Node<T> *)my_allocator_.newNode();
        q_head = newNode;
        q_tail = newNode;
        // my_allocator_.freeNode(newNode);
    }

    void enqueue(T value)
    {
        Node<T> *newNode = (Node<T> *)my_allocator_.newNode();
        newNode->value = value;
        newNode->next = NULL;

        mtx.lock();
        q_tail->next = newNode;
        q_tail = newNode;
        mtx.unlock();
    }

    bool dequeue(T *value)
    {
        bool ret_value = false;
        mtx.lock();
        Node<T> *new_head = q_head->next;
        if (new_head == NULL)
        {
            mtx.unlock();
            return ret_value;
        }
        *value = new_head->value;
        q_head = new_head;

        mtx.unlock();
        // return ret_value;
        return true;
    }

    void cleanup()
    {
        my_allocator_.cleanup();
    }
};