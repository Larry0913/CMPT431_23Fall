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
class OneLockStack
{
    std::mutex mtx;
    Node<T> *top_stack;
    CustomAllocator my_allocator_;

public:
    OneLockStack() : my_allocator_()
    {
        std::cout << "Using OneLockStack\n";
    }

    void initStack(long t_my_allocator_size)
    {
        std::cout << "Using Allocator\n";
        my_allocator_.initialize(t_my_allocator_size, sizeof(Node<T>));
        // Perform any necessary initializations
        Node<T> *node = (Node<T> *)my_allocator_.newNode();
        node->next = NULL;
        top_stack = node;
    }

    /**
     * Create a new node with value `value` and update it to be the top of the stack.
     * This operation must always succeed.
     */
    void push(T value)
    {
        mtx.lock();
        Node<T> *node = (Node<T> *)my_allocator_.newNode();
        node->value = value;
        node->next = top_stack;
        top_stack = node;
        mtx.unlock();
    }

    /**
     * If the stack is empty: return false.
     * Otherwise: copy the value at the top of the stack into `value`, update
     * the top to point to the next element in the stack, and return true.
     */
    bool pop(T *value)
    {
        mtx.lock();
        if (top_stack->next == NULL)
        {
            mtx.unlock();
            return false;
        }
        *value = top_stack->value;
        top_stack = top_stack->next;
        mtx.unlock();
        return true;
    }

    void cleanup()
    {
        my_allocator_.cleanup();
    }
};
