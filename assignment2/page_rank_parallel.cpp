#include "core/graph.h"
#include "core/utils.h"
#include <iomanip>
#include <iostream>
#include <stdlib.h>
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

Graph g;
std::atomic<uintV> current_vertex(0);

void pageRankSerial(Graph &g, int max_iters)
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
    // Create threads and distribute the work across T threads
    // -------------------------------------------------------------------
    t1.start();
    for (int iter = 0; iter < max_iters; iter++)
    {
        // for each vertex 'u', process all its outNeighbors 'v'
        for (uintV u = 0; u < n; u++)
        {
            uintE out_degree = g.vertices_[u].getOutDegree();
            for (uintE i = 0; i < out_degree; i++)
            {
                uintV v = g.vertices_[u].getOutNeighbor(i);
                pr_next[v] += (pr_curr[u] / out_degree);
            }
        }
        for (uintV v = 0; v < n; v++)
        {
            pr_next[v] = PAGE_RANK(pr_next[v]);

            // reset pr_curr for the next iteration
            pr_curr[v] = pr_next[v];
            pr_next[v] = 0.0;
        }
    }
    time_taken = t1.stop();
    // -------------------------------------------------------------------

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

void pageRankThread1(PageRankType *pr_curr, std::atomic<PageRankType> *pr_next, uintV start, uintV end, CustomBarrier *my_barrier, int max_iterations, int thread_id)
{
    double parallel_time = 0.0;
    timer t1;
    double barrier1_time = 0.0;
    timer t2;
    double barrier2_time = 0.0;
    timer t3;
    int getNextVertex_time = 0;

    uintV num_vertices = 0;
    uintE num_edges = 0;

    t1.start();

    for (int iter = 0; iter < max_iterations; iter++)
    {
        // for each vertex 'u', process all its outNeighbors 'v'
        for (uintV u = start; u < end; u++)
        {
            uintE out_degree = g.vertices_[u].getOutDegree();
            num_edges += out_degree;
            for (uintE i = 0; i < out_degree; i++)
            {
                uintV v = g.vertices_[u].getOutNeighbor(i);
                PageRankType contribution = pr_curr[u] / out_degree;

                // Use compare_exchange to atomically update pr_next[v]
                PageRankType old_value = pr_next[v].load();
                PageRankType new_value;
                do
                {
                    new_value = old_value + contribution;
                } while (!pr_next[v].compare_exchange_weak(old_value, new_value));
            }
        }
        t2.start();
        my_barrier->wait();
        barrier1_time += t2.stop();
        for (uintV v = start; v < end; v++)
        {
            pr_next[v] = PAGE_RANK(pr_next[v]);
            // Reset pr_curr for the next iteration
            pr_curr[v] = pr_next[v];
            pr_next[v] = 0.0;
            num_vertices++;
        }
        t3.start();
        my_barrier->wait();
        barrier2_time += t3.stop();
    }

    parallel_time = t1.stop();
    // std::cout << thread_id << ", " << parallel_time << "\n";
    // printf("%d, %lf\n", thread_id, parallel_time);
    printf("%d, %d, %d, %lf, %lf, %d, %lf\n", thread_id, num_vertices, num_edges, barrier1_time, barrier2_time, getNextVertex_time, parallel_time);
}

void pageRankParallel1(Graph &g, int max_iterations, uint n_workers)
{
    uintV n = g.n_;

    PageRankType *pr_curr = new PageRankType[n];
    std::atomic<PageRankType> *pr_next = new std::atomic<PageRankType>[n];

    for (uintV i = 0; i < n; i++)
    {
        pr_curr[i] = INIT_PAGE_RANK;
        pr_next[i] = 0.0;
    }

    // Push based pagerank
    double time_taken = 0.0;
    timer t1;
    double partition_time = 0.0;
    timer t2;
    std::vector<std::thread> vector_threads;
    CustomBarrier my_barrier(n_workers);
    uintV thread_points = n / n_workers;
    uintV start = 0;
    uintV end = 0;

    // Create threads and distribute the work across T threads
    // -------------------------------------------------------------------
    // std::cout << "thread_id, time_taken\n";
    std::cout << "thread_id, num_vertices, num_edges, barrier1_time, barrier2_time, getNextVertex_time, total_time\n";
    t1.start();
    for (int i = 0; i < n_workers; i++)
    {
        t2.start();
        start = i * thread_points;
        end = (i + 1) * thread_points;
        if (i == n_workers - 1)
        {
            end = n;
        }
        partition_time += t2.stop();
        vector_threads.push_back(std::thread(pageRankThread1, pr_curr, pr_next, start, end, &my_barrier, max_iterations, i));
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
    std::cout << "Partitioning time (in seconds) : " << std::setprecision(6) << partition_time << "\n";
    std::cout << "Time taken (in seconds) : " << std::setprecision(6) << time_taken << "\n";
    delete[] pr_curr;
    delete[] pr_next;
}

void pageRankThread2(PageRankType *pr_curr, std::atomic<PageRankType> *pr_next, uintV start, uintV end, CustomBarrier *my_barrier, int max_iterations, int thread_id)
{
    double parallel_time = 0.0;
    timer t1;
    double barrier1_time = 0.0;
    timer t2;
    double barrier2_time = 0.0;
    timer t3;
    int getNextVertex_time = 0;

    uintV num_vertices = 0;
    uintE num_edges = 0;

    t1.start();

    for (int iter = 0; iter < max_iterations; iter++)
    {
        // for each vertex 'u', process all its outNeighbors 'v'
        for (uintV u = start; u < end; u++)
        {
            uintE out_degree = g.vertices_[u].getOutDegree();
            num_edges += out_degree;
            for (uintE i = 0; i < out_degree; i++)
            {
                uintV v = g.vertices_[u].getOutNeighbor(i);
                PageRankType contribution = pr_curr[u] / out_degree;

                // Use compare_exchange to atomically update pr_next[v]
                PageRankType old_value = pr_next[v].load();
                PageRankType new_value;
                do
                {
                    new_value = old_value + contribution;
                } while (!pr_next[v].compare_exchange_weak(old_value, new_value));
            }
        }
        t2.start();
        my_barrier->wait();
        barrier1_time += t2.stop();
        for (uintV v = start; v < end; v++)
        {
            pr_next[v] = PAGE_RANK(pr_next[v]);
            // Reset pr_curr for the next iteration
            pr_curr[v] = pr_next[v];
            pr_next[v] = 0.0;
            num_vertices++;
        }
        t3.start();
        my_barrier->wait();
        barrier2_time += t3.stop();
    }

    parallel_time = t1.stop();
    // std::cout << thread_id << ", " << parallel_time << "\n";
    // printf("%d, %lf\n", thread_id, parallel_time);
    printf("%d, %d, %d, %lf, %lf, %d, %lf\n", thread_id, num_vertices, num_edges, barrier1_time, barrier2_time, getNextVertex_time, parallel_time);
}

void pageRankParallel2(Graph &g, int max_iterations, uint n_workers)
{
    uintV n = g.n_;
    uintE m = g.m_;

    PageRankType *pr_curr = new PageRankType[n];
    std::atomic<PageRankType> *pr_next = new std::atomic<PageRankType>[n];

    for (uintV i = 0; i < n; i++)
    {
        pr_curr[i] = INIT_PAGE_RANK;
        pr_next[i] = 0.0;
    }

    // Push based pagerank
    double time_taken = 0.0;
    timer t1;
    double partition_time = 0.0;
    timer t2;
    std::vector<std::thread> vector_threads;
    CustomBarrier my_barrier(n_workers);
    uintV thread_points = m / n_workers;
    uintV start = 0;
    uintV end = 0;
    int thread_id = 0;
    uintE edge_base = 0;

    // Create threads and distribute the work across T threads
    // -------------------------------------------------------------------
    // std::cout << "thread_id, time_taken\n";
    std::cout << "thread_id, num_vertices, num_edges, barrier1_time, barrier2_time, getNextVertex_time, total_time\n";
    t1.start();

    for (int i = 0; i < n; i++)
    {
        t2.start();
        if (edge_base <= thread_points)
        {
            edge_base += g.vertices_[i].getOutDegree();
        }

        else
        {
            end = i - 1;
            partition_time += t2.stop();
            vector_threads.push_back(std::thread(pageRankThread2, pr_curr, pr_next, start, end, &my_barrier, max_iterations, thread_id));
            start = end;
            edge_base = 0;
            thread_id++;
        }

        if (thread_id == n_workers - 1)
        {
            end = n;
            partition_time += t2.stop();
            vector_threads.push_back(std::thread(pageRankThread2, pr_curr, pr_next, start, end, &my_barrier, max_iterations, thread_id));
            break;
        }
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
    std::cout << "Partitioning time (in seconds) : " << std::setprecision(6) << partition_time << "\n";
    std::cout << "Time taken (in seconds) : " << std::setprecision(6) << time_taken << "\n";
    delete[] pr_curr;
    delete[] pr_next;
}

uintV getNextVertexToBeProcessed(uintV n, uint granularity)
{
    uintV current = current_vertex.fetch_add(granularity);
    if (current >= n)
        return -1;
    else
        return current;
}

void pageRankThread3(PageRankType *pr_curr, std::atomic<PageRankType> *pr_next, CustomBarrier *my_barrier, int max_iterations, int thread_id, uint granularity)
{
    uintV n = g.n_;
    double parallel_time = 0.0;
    timer t1;
    double barrier1_time = 0.0;
    timer t2;
    double barrier2_time = 0.0;
    timer t3;
    double getNextVertex_time = 0;
    timer t4;

    uintV num_vertices = 0;
    uintE num_edges = 0;
    t1.start();

    for (int iter = 0; iter < max_iterations; iter++)
    {
        while (true)
        {
            t4.start();
            uintV u = getNextVertexToBeProcessed(n, granularity);
            getNextVertex_time += t4.stop();

            if (u == -1)
            {
                break;
            }

            // for each vertex 'u', process all its outNeighbors 'v'
            for (int j = 0; j < granularity; j++)
            {
                uintE out_degree = g.vertices_[u].getOutDegree();
                num_edges += out_degree;
                for (uintE i = 0; i < out_degree; i++)
                {
                    uintV v = g.vertices_[u].getOutNeighbor(i);
                    PageRankType contribution = pr_curr[u] / out_degree;

                    // Use compare_exchange to atomically update pr_next[v]
                    PageRankType old_value = pr_next[v].load();
                    PageRankType new_value;
                    do
                    {
                        new_value = old_value + contribution;
                    } while (!pr_next[v].compare_exchange_weak(old_value, new_value));
                }
                u++;

                if (u >= n)
                {
                    break; // n is the total number of vertices in the graph
                }
            }
        }

        t2.start();
        my_barrier->wait();
        barrier1_time += t2.stop();

        if (thread_id == 0)
        {
            current_vertex = 0;
        }
        my_barrier->wait();

        while (true)
        {
            t4.start();
            uintV v = getNextVertexToBeProcessed(n, granularity);
            getNextVertex_time += t4.stop();

            if (v == -1)
            {
                break;
            }

            for (int j = 0; j < granularity; j++)
            {
                pr_next[v] = PAGE_RANK(pr_next[v]);
                // Reset pr_curr for the next iteration
                pr_curr[v] = pr_next[v];
                pr_next[v] = 0.0;
                num_vertices++;

                v++;
                if (v >= n)
                {
                    break; // n is the total number of vertices in the graph
                }
            }
        }

        t3.start();
        my_barrier->wait();
        barrier2_time += t3.stop();

        if (thread_id == 0)
        {
            current_vertex = 0;
        }
        my_barrier->wait();
    }

    parallel_time = t1.stop();
    // std::cout << thread_id << ", " << parallel_time << "\n";
    // printf("%d, %lf\n", thread_id, parallel_time);
    printf("%d, %d, %d, %lf, %lf, %lf, %lf\n", thread_id, num_vertices, num_edges, barrier1_time, barrier2_time, getNextVertex_time, parallel_time);
}

void pageRankParallel3(Graph &g, int max_iterations, uint n_workers, uint granularity)
{
    uintV n = g.n_;

    PageRankType *pr_curr = new PageRankType[n];
    std::atomic<PageRankType> *pr_next = new std::atomic<PageRankType>[n];

    for (uintV i = 0; i < n; i++)
    {
        pr_curr[i] = INIT_PAGE_RANK;
        pr_next[i] = 0.0;
    }

    // Push based pagerank
    double time_taken = 0.0;
    timer t1;
    double partition_time = 0.0;
    timer t2;
    std::vector<std::thread> vector_threads;
    CustomBarrier my_barrier(n_workers);

    // Create threads and distribute the work across T threads
    // -------------------------------------------------------------------
    // std::cout << "thread_id, time_taken\n";
    std::cout << "thread_id, num_vertices, num_edges, barrier1_time, barrier2_time, getNextVertex_time, total_time\n";
    // t1.start();
    for (int i = 0; i < n_workers; i++)
    {
        // std::cout<< start_vertex[i]<<","<<end_vertex[i]<<std::endl;
        vector_threads.push_back(std::thread(pageRankThread3, pr_curr, pr_next, &my_barrier, max_iterations, i, granularity));
    }
    t1.start();
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
    std::cout << "Partitioning time (in seconds) : " << std::setprecision(6) << partition_time << "\n";
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
            {"strategy", "Strategy to be used",
             cxxopts::value<uint>()->default_value(DEFAULT_STRATEGY)},
            {"granularity", "Granularity to be used",
             cxxopts::value<uint>()->default_value(DEFAULT_GRANULARITY)},
            {"inputFile", "Input graph file path",
             cxxopts::value<std::string>()->default_value(
                 "/scratch/input_graphs/roadNet-CA")},
        });

    auto cl_options = options.parse(argc, argv);
    uint n_workers = cl_options["nWorkers"].as<uint>();
    uint strategy = cl_options["strategy"].as<uint>();
    uint max_iterations = cl_options["nIterations"].as<uint>();
    uint granularity = cl_options["granularity"].as<uint>();
    std::string input_file_path = cl_options["inputFile"].as<std::string>();

#ifdef USE_INT
    std::cout << "Using INT\n";
#else
    std::cout << "Using FLOAT\n";
#endif
    std::cout << std::fixed;
    std::cout << "Number of workers : " << n_workers << "\n";
    std::cout << "Task decomposition strategy : " << strategy << "\n";
    std::cout << "Iterations : " << max_iterations << "\n";
    std::cout << "Granularity : " << granularity << "\n";

    // Graph g;
    std::cout << "Reading graph\n";
    g.readGraphFromBinary<int>(input_file_path);
    std::cout << "Created graph\n";
    switch (strategy)
    {
    case 0:
        std::cout << "\nSerial\n";
        pageRankSerial(g, max_iterations);
        break;
    case 1:
        std::cout << "\nVertex-based work partitioning\n";
        pageRankParallel1(g, max_iterations, n_workers);
        break;
    case 2:
        std::cout << "\nEdge-based work partitioning\n";
        pageRankParallel2(g, max_iterations, n_workers);
        break;
    case 3:
        std::cout << "\nDynamic task mapping\n";
        pageRankParallel3(g, max_iterations, n_workers, granularity);
        break;
    default:
        break;
    }

    return 0;
}
