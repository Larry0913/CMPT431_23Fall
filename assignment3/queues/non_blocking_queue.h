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
class NonBlockingQueue
{
    pointer_t<Node<T>> q_head;
    pointer_t<Node<T>> q_tail;
    CustomAllocator my_allocator_;

public:
    NonBlockingQueue() : my_allocator_()
    {
        std::cout << "Using NonBlockingQueue\n";
    }

    void initQueue(long allocator_init)
    {
        std::cout << "Using Allocator\n";
        my_allocator_.initialize(allocator_init, sizeof(Node<T>));
        Node<T> *node = (Node<T> *)my_allocator_.newNode();
        // my_allocator_.freeNode(newNode);
        node->next.ptr = NULL;
        q_head.ptr = node;
        q_tail.ptr = node;
    }

    void enqueue(T value)
    {
        Node<T> *node = (Node<T> *)my_allocator_.newNode();
        node->value = value;
        node->next.ptr = NULL;
        SFENCE;
        pointer_t<Node<T>> tail;
        pointer_t<Node<T>> next;
        pointer_t<Node<T>> next_ptr;

        while (true)
        {
            tail = q_tail;
            LFENCE;
            next = q_tail.address()->next;
            LFENCE;
            if (tail.ptr == q_tail.ptr)
            {
                if (next.address() == NULL)
                {
                    // if(CAS(&tail.address()->next, next, <newNode, next.count()+1>))
                    // Extract the 16 most significant bits of the count from the 'next' pointer and increment it by 1.
                    uintptr_t count_16 = static_cast<uintptr_t>(next.count() + 1) << 48;

                    // Create a new pointer 'next_ptr' by combining the existing 'node' pointer
                    // with the updated count in the 16 most significant bits.
                    next_ptr.ptr = reinterpret_cast<Node<int> *>((reinterpret_cast<uintptr_t>(node) & ((1ull << 48) - 1)) | count_16);

                    if (CAS(&tail.address()->next, next, next_ptr))
                        break;
                }
                else
                {
                    // CAS(&q_tail, tail, <next.address(), tail.count()+1>);// ELABEL
                    uintptr_t count_16 = static_cast<uintptr_t>(tail.count() + 1) << 48;
                    next_ptr.ptr = reinterpret_cast<Node<int> *>((reinterpret_cast<uintptr_t>(next.address()) & ((1ull << 48) - 1)) | count_16);
                    CAS(&q_tail, tail, next_ptr); // ELABEL
                }
            }
        }
        SFENCE;
        uintptr_t count_16 = static_cast<uintptr_t>(tail.count() + 1) << 48;
        next_ptr.ptr = reinterpret_cast<Node<int> *>((reinterpret_cast<uintptr_t>(node) & ((1ull << 48) - 1)) | count_16);
        CAS(&q_tail, tail, next_ptr);
    }

    bool dequeue(T *value)
    {
        // bool ret_value = false;
        // return ret_value;
        pointer_t<Node<T>> head;
        pointer_t<Node<T>> tail;
        pointer_t<Node<T>> next;
        pointer_t<Node<T>> next_ptr;
        while (true)
        {
            head = q_head;
            LFENCE;
            tail = q_tail;
            LFENCE;
            next = head.address()->next;
            LFENCE;
            if (head.ptr == q_head.ptr)
            {
                if (head.address() == tail.address())
                {
                    if (next.address() == NULL)
                        return false;
                    // CAS(&q_tail, tail, <next.address(), tail.count()+1>);	//DLABEL
                    uintptr_t count_16 = static_cast<uintptr_t>(tail.count() + 1) << 48;
                    next_ptr.ptr = reinterpret_cast<Node<int> *>((reinterpret_cast<uintptr_t>(next.address()) & ((1ull << 48) - 1)) | count_16);
                    CAS(&q_tail, tail, next_ptr); // DLABEL
                }
                else
                {
                    *value = next.address()->value;
                    // if(CAS(&q_head, head, <next.address(), head.count()+1>))
                    uintptr_t count_16 = static_cast<uintptr_t>(head.count() + 1) << 48;
                    next_ptr.ptr = reinterpret_cast<Node<int> *>((reinterpret_cast<uintptr_t>(next.address()) & ((1ull << 48) - 1)) | count_16);
                    if (CAS(&q_head, head, next_ptr))
                    {
                        my_allocator_.freeNode(head.address());
                        return true;
                    }
                }
            }
        }
    }

    void cleanup()
    {
        my_allocator_.cleanup();
    }
};
