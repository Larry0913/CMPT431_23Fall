#include "../common/allocator.h"

#define LFENCE asm volatile("lfence" \
                            :        \
                            :        \
                            : "memory")
#define SFENCE asm volatile("sfence" \
                            :        \
                            :        \
                            : "memory")

template <class P>
struct pointer_t
{
public:
    P *ptr;

    P *address()
    {
        // Get the address by getting the 48 least significant bits of ptr
        // Create a mask with the 48 least significant bits set to 1
        uintptr_t mask = (1ull << 48) - 1;
        return reinterpret_cast<P *>(reinterpret_cast<uintptr_t>(ptr) & mask);
    }
    uint count()
    {
        // Get the count from the 16 most significant bits of ptr
        // Create a mask with the 16 most significant bits set to 1 and shifted to the left
        uintptr_t mask = ((1ull << 16) - 1) << 48;
        return static_cast<uint>((reinterpret_cast<uintptr_t>(ptr) & mask) >> 48);
    }
};

template <class T>
class Node
{
public:
    T value;
    pointer_t<Node<T>> next;
};

template <class T>
class LockFreeStack
{
    pointer_t<Node<T>> top_stack;
    CustomAllocator my_allocator_;

private:
    bool tryPush(Node<T> *node)
    {
        pointer_t<Node<T>> top;
        pointer_t<Node<T>> next_ptr;

        top = top_stack;
        LFENCE;
        node->next.ptr = top.address();

        // next_ptr.ptr = node;
        if (top.ptr == top_stack.ptr)
        {
            uintptr_t count_16 = static_cast<uintptr_t>(top.count() + 1) << 48;
            next_ptr.ptr = reinterpret_cast<Node<int> *>((reinterpret_cast<uintptr_t>(node) & ((1ull << 48) - 1)) | count_16);
            if (CAS(&top_stack, top, next_ptr))
            {
                return true;
            }
        }

        return false;
    }

public:
    LockFreeStack() : my_allocator_()
    {
        std::cout << "Using LockFreeStack\n";
    }

    void initStack(long t_my_allocator_size)
    {
        std::cout << "Using Allocator\n";
        my_allocator_.initialize(t_my_allocator_size, sizeof(Node<T>));
        // Perform any necessary initializations
        Node<T> *node = (Node<T> *)my_allocator_.newNode();
        node->next.ptr = NULL;
        top_stack.ptr = node;
    }

    /**
     * Create a new node with value `value` and update it to be the top of the stack.
     * This operation must always succeed.
     */

    void push(T value)
    {
        Node<T> *node = (Node<T> *)my_allocator_.newNode();
        node->value = value;
        node->next.ptr = nullptr;
        SFENCE;
        while (true)
        {
            if (tryPush(node))
            {
                return;
            }
        }
    }

    /**
     * If the stack is empty: return false.
     * Otherwise: copy the value at the top of the stack into `value`, update
     * the top to point to the next element in the stack, and return true.
     */
    bool pop(T *value)
    {
        pointer_t<Node<T>> top;
        pointer_t<Node<T>> next_ptr;
        pointer_t<Node<T>> next;
        while (true)
        {
            top = top_stack;
            LFENCE;
            next = top_stack.address()->next;

            if (top.ptr == top_stack.ptr)
            {
                if (next.address() == nullptr)
                {
                    //*value = NULL;
                    return false;
                }
                uintptr_t count_16 = static_cast<uintptr_t>(top.count() + 1) << 48;
                next_ptr.ptr = reinterpret_cast<Node<int> *>((reinterpret_cast<uintptr_t>(next.address()) & ((1ull << 48) - 1)) | count_16);
                if (CAS(&top_stack, top, next_ptr))
                {
                    *value = top.address()->value;
                    my_allocator_.freeNode(top.address());
                    return true;
                }
            }
        }

        // return false;
    }

    void cleanup()
    {
        my_allocator_.cleanup();
    }
};
