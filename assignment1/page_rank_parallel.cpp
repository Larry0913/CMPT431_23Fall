#include "core/graph.h"
#include "core/utils.h"
#include <iomanip>
#include <iostream>
#include <stdlib.h>
#include <mutex>
#include <vector>
#include <thread>

#ifdef USE_INT
#define INIT_PAGE_RANK 100000
#define EPSILON 1000
#define PAGE_RANK(x) (15000 + (5 * x) / 6)
#define CHANGE_IN_PAGE_RANK(x, y) std::abs(x - y)
typedef int64_t PageRankType;
#else
#define INIT_PAGE_RANK 1.0
#define EPSILON 0.01
#define DAMPING 0.85
#define PAGE_RANK(x) (1 - DAMPING + DAMPING * x)
#define CHANGE_IN_PAGE_RANK(x, y) std::fabs(x - y)
typedef float PageRankType;
#endif

std::mutex mtx;
Graph g;

void pageRankCalParallel(PageRankType *pr_curr, PageRankType *pr_next, uintV start, uintV end, CustomBarrier *my_barrier,
                         int max_iters, int thread_id)
{
    timer t2;
    uintV n = g.n_;
    double parallel_time = 0.0;
    // uint index_lock = 0;
    // PageRankType not_in_range[g.n_] = {};
    PageRankType *not_in_range = new PageRankType[n];

    for (int i = 0; i < n; i++)
    {
        not_in_range[i] = 0;
    }

    t2.start();

    for (int iter = 0; iter < max_iters; iter++)
    {
        // for each vertex 'u', process all its outNeighbors 'v'
        for (uintV u = start; u < end; u++)
        {
            uintE out_degree = g.vertices_[u].getOutDegree();
            for (uintE i = 0; i < out_degree; i++)
            {
                uintV v = g.vertices_[u].getOutNeighbor(i);
                // if (v < start || v > end)
                // {
                not_in_range[v] += pr_curr[u] / out_degree;
                // mtx.lock();
                // pr_next[v] += (pr_curr[u] / out_degree);
                // mtx.unlock();
                // }
                // else
                // {
                //     pr_next[v] += (pr_curr[u] / out_degree);
                // }

                // index_lock = v / (end - start);
                // (*vertex_mutexes)[index_lock].lock();
                // pr_next[v] += (pr_curr[u] / out_degree);
                // (*vertex_mutexes)[index_lock].unlock();
            }
        }

        mtx.lock();
        for (int i = 0; i < n; i++)
        {
            // if (i < start || i > end)
            pr_next[i] += not_in_range[i];
        }
        mtx.unlock();

        my_barrier->wait();

        for (uintV v = start; v < end; v++)
        {
            // index_lock = v / (end - start);
            //(*vertex_mutexes)[index_lock].lock();
            pr_next[v] = PAGE_RANK(pr_next[v]);
            //(*vertex_mutexes)[index_lock].unlock();
            //   reset pr_curr for the next iteration
            pr_curr[v] = pr_next[v];
            pr_next[v] = 0.0;
        }

        for (int i = 0; i < n; i++)
        {
            not_in_range[i] = 0;
        }

        my_barrier->wait();
    }

    parallel_time = t2.stop();
    // std::cout << thread_id << ", " << parallel_time << "\n";
    printf("%d, %lf\n", thread_id, parallel_time);
    delete[] not_in_range;
}

void pageRankParallel(Graph &g, int max_iters, uint n_workers)
{
    uintV n = g.n_;

    PageRankType *pr_curr = new PageRankType[n];
    PageRankType *pr_next = new PageRankType[n];

    for (uintV i = 0; i < n; i++)
    {
        pr_curr[i] = INIT_PAGE_RANK;
        pr_next[i] = 0.0;
    }

    // Push based pagerank
    timer t1;
    double time_taken = 0.0;
    std::vector<std::thread> vector_threads;
    CustomBarrier my_barrier(n_workers);
    // std::vector<std::mutex> vertex_mutexes(n_workers);
    uintV thread_points = n / n_workers;
    uintV start = 0;
    uintV end = 0;

    // Create threads and distribute the work across T threads
    // -------------------------------------------------------------------
    std::cout << "thread_id, time_taken\n";
    t1.start();
    for (int i = 0; i < n_workers; i++)
    {
        start = i * thread_points;
        end = (i + 1) * thread_points;
        if (i == n_workers - 1)
        {
            end = n;
        }
        vector_threads.push_back(std::thread(pageRankCalParallel, pr_curr, pr_next, start, end, &my_barrier, max_iters, i));
        // vector_threads.push_back(std::thread(pageRankCalParallel, pr_curr, pr_next, start, end, &my_barrier, max_iters, &vertex_mutexes, i));
    }

    for (int i = 0; i < n_workers; i++)
    {
        vector_threads[i].join();
    }

    time_taken = t1.stop();
    // -------------------------------------------------------------------
    // Print the above statistics for each thread
    // Example output for 2 threads:
    // thread_id, time_taken
    // 0, 0.12
    // 1, 0.12

    PageRankType sum_of_page_ranks = 0;
    for (uintV u = 0; u < n; u++)
    {
        sum_of_page_ranks += pr_curr[u];
    }
    std::cout << "Sum of page rank : " << sum_of_page_ranks << "\n";
    std::cout << "Time taken (in seconds) : " << time_taken << "\n";
    delete[] pr_curr;
    delete[] pr_next;
}

int main(int argc, char *argv[])
{
    cxxopts::Options options(
        "page_rank_push",
        "Calculate page_rank using serial and parallel execution");
    options.add_options(
        "",
        {
            {"nWorkers", "Number of workers",
             cxxopts::value<uint>()->default_value(DEFAULT_NUMBER_OF_WORKERS)},
            {"nIterations", "Maximum number of iterations",
             cxxopts::value<uint>()->default_value(DEFAULT_MAX_ITER)},
            {"inputFile", "Input graph file path",
             cxxopts::value<std::string>()->default_value(
                 "/scratch/input_graphs/roadNet-CA")},
        });

    auto cl_options = options.parse(argc, argv);
    uint n_workers = cl_options["nWorkers"].as<uint>();
    uint max_iterations = cl_options["nIterations"].as<uint>();
    std::string input_file_path = cl_options["inputFile"].as<std::string>();

#ifdef USE_INT
    std::cout << "Using INT\n";
#else
    std::cout << "Using FLOAT\n";
#endif
    std::cout << std::fixed;
    std::cout << "Number of workers : " << n_workers << "\n";

    // Graph g;
    std::cout << "Reading graph\n";
    g.readGraphFromBinary<int>(input_file_path);
    std::cout << "Created graph\n";
    pageRankParallel(g, max_iterations, n_workers);

    return 0;
}
