#include <iostream>
#include <cstdio>
#include "core/utils.h"
#include "core/graph.h"
#include <mpi.h>

#ifdef USE_INT
#define INIT_PAGE_RANK 100000
#define EPSILON 1000
#define PAGE_RANK(x) (15000 + (5 * x) / 6)
#define CHANGE_IN_PAGE_RANK(x, y) std::abs(x - y)
#define PAGERANK_MPI_TYPE MPI_LONG
#define PR_FMT "%ld"
typedef int64_t PageRankType;
#else
#define INIT_PAGE_RANK 1.0
#define EPSILON 0.01
#define DAMPING 0.85
#define PAGE_RANK(x) (1 - DAMPING + DAMPING * x)
#define CHANGE_IN_PAGE_RANK(x, y) std::fabs(x - y)
#define PAGERANK_MPI_TYPE MPI_FLOAT
#define PR_FMT "%f"
typedef float PageRankType;
#endif

void pageRankSerial(Graph &g, int max_iters)
{
    uintV n = g.n_;
    double time_taken;
    timer t1;
    PageRankType *pr_curr = new PageRankType[n];
    PageRankType *pr_next = new PageRankType[n];

    t1.start();
    for (uintV i = 0; i < n; i++)
    {
        pr_curr[i] = INIT_PAGE_RANK;
        pr_next[i] = 0.0;
    }

    // Push based pagerank
    // -------------------------------------------------------------------
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
    // -------------------------------------------------------------------

    // For every thread, print the following statistics:
    // rank, num_edges, communication_time
    // 0, 344968860, 1.297778
    // 1, 344968860, 1.247763
    // 2, 344968860, 0.956243
    // 3, 344968880, 0.467028

    PageRankType sum_of_page_ranks = 0;
    for (uintV u = 0; u < n; u++)
    {
        sum_of_page_ranks += pr_curr[u];
    }
    time_taken = t1.stop();
    std::printf("Sum of page rank : " PR_FMT "\n", sum_of_page_ranks);
    std::printf("Time taken (in seconds) : %f\n", time_taken);
    delete[] pr_curr;
    delete[] pr_next;
}

void pageRankParallelStrategy1(Graph &g, int max_iterations, int global_rank, int world_size)
{
    uintV n = g.n_;
    uintE m = g.m_;

    uintE edges_processed = 0;
    int startCount = 0;
    int endCount = 0;

    uintV *startVector = new uintV[world_size];
    uintV *endVector = new uintV[world_size];
    startVector[0] = 0;
    int *tempVec = new int[world_size];
    tempVec[0] = 0;

    PageRankType *pr_curr = new PageRankType[n];
    PageRankType *pr_next = new PageRankType[n];
    PageRankType *pr_recv = new PageRankType[n];

    double time_taken = 0.0;
    timer timer0;
    double communicate_time = 0;
    timer timer1;

    if (global_rank == 0)
    {
        timer0.start();
    }

    for (uintV i = 0; i < n; i++)
    {
        pr_curr[i] = INIT_PAGE_RANK;
        pr_next[i] = 0.0;
        pr_recv[i] = 0.0;
    }

    // assigning vertex using edge decomposition
    for (int i = 0; i < world_size; i++)
    {
        long local_count = 0;
        startCount = endCount;
        if (i != 0)
        {
            startVector[i] = endVector[i - 1];
        }
        while (endCount < n)
        {
            local_count += g.vertices_[endCount].getOutDegree();
            endCount += 1;
            if (local_count >= m / world_size)
                break;
        }
        endVector[i] = endCount;
        endVector[world_size - 1] = n;
    }

    for (int i = 1; i < world_size; i++)
    {
        tempVec[i] = endVector[i] - startVector[i];
    }

    for (int iter = 0; iter < max_iterations; iter++)
    {
        // Loop 1
        // for each vertex 'u', process all its outNeighbors 'v'
        for (uintV u = startVector[global_rank]; u < endVector[global_rank]; u++)
        {
            uintE out_degree = g.vertices_[u].getOutDegree();
            edges_processed += out_degree;
            for (uintE i = 0; i < out_degree; i++)
            {
                uintV v = g.vertices_[u].getOutNeighbor(i);
                pr_next[v] += (pr_curr[u] / out_degree);
            }
        }

        // --- synchronization phase 1 start ---
        timer1.start();
        MPI_Reduce(pr_next, pr_recv, n, PAGERANK_MPI_TYPE, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Scatterv(pr_recv, tempVec, startVector, PAGERANK_MPI_TYPE, pr_recv, endVector[global_rank] - startVector[global_rank], PAGERANK_MPI_TYPE, 0, MPI_COMM_WORLD);
        for (int i = startVector[global_rank]; i < endVector[global_rank]; i++)
        {
            pr_next[i] = pr_recv[i - startVector[global_rank]];
        }
        communicate_time += timer1.stop();
        // --- synchronization phase 1 end -----

        // Loop 2
        for (uintV v = startVector[global_rank]; v < endVector[global_rank]; v++)
        {
            PageRankType new_value = PAGE_RANK(pr_next[v]);
            pr_curr[v] = new_value;
        }

        // Reset next_page_rank[v] to 0 for all vertices
        for (uintV v = 0; v < n; v++)
        {
            pr_next[v] = 0.0;
            pr_recv[v] = 0.0;
        }
    }

    PageRankType local_sum = 0;

    // loop 3
    for (uintV v = startVector[global_rank]; v < endVector[global_rank]; v++)
    {
        local_sum += pr_curr[v];
    }

    // --- synchronization phase 2 start ---
    PageRankType total_page_rank = 0;
    MPI_Reduce(&local_sum, &total_page_rank, 1, PAGERANK_MPI_TYPE, MPI_SUM, 0, MPI_COMM_WORLD);
    // --- synchronization phase 2 end --

    std::printf("%d, %u, %lf\n", global_rank, edges_processed, communicate_time);

    if (global_rank == 0)
    {
        time_taken = timer0.stop();
        std::printf("Sum of page rank : " PR_FMT "\n", total_page_rank);
        std::printf("Time taken (in seconds) : %f\n", time_taken);
    }

    delete[] startVector;
    delete[] endVector;
    delete[] pr_curr;
    delete[] pr_next;
    delete[] pr_recv;
}

void pageRankParallelStrategy2(Graph &g, int max_iterations, int global_rank, int world_size)
{
    uintV n = g.n_;
    uintE m = g.m_;

    uintE edges_processed = 0;
    int startCount = 0;
    int endCount = 0;

    uintV *startVector = new uintV[world_size];
    uintV *endVector = new uintV[world_size];
    startVector[0] = 0;
    int *tempVec = new int[world_size];
    tempVec[0] = 0;

    PageRankType *pr_curr = new PageRankType[n];
    PageRankType *pr_next = new PageRankType[n];
    PageRankType *pr_recv = new PageRankType[n];

    double time_taken = 0.0;
    timer timer0;
    double communicate_time = 0;
    timer timer1;

    if (global_rank == 0)
    {
        timer0.start();
    }

    for (uintV i = 0; i < n; i++)
    {
        pr_curr[i] = INIT_PAGE_RANK;
        pr_next[i] = 0.0;
        pr_recv[i] = 0.0;
    }

    // assigning vertex using edge decomposition
    for (int i = 0; i < world_size; i++)
    {
        long local_count = 0;
        startCount = endCount;
        if (i != 0)
        {
            startVector[i] = endVector[i - 1];
        }
        while (endCount < n)
        {
            local_count += g.vertices_[endCount].getOutDegree();
            endCount += 1;
            if (local_count >= m / world_size)
                break;
        }
        endVector[i] = endCount;
        endVector[world_size - 1] = n;
    }

    for (int i = 1; i < world_size; i++)
    {
        tempVec[i] = endVector[i] - startVector[i];
    }

    for (int iter = 0; iter < max_iterations; iter++)
    {
        // Loop 1
        // for each vertex 'u', process all its outNeighbors 'v'
        for (uintV u = startVector[global_rank]; u < endVector[global_rank]; u++)
        {
            uintE out_degree = g.vertices_[u].getOutDegree();
            edges_processed += out_degree;
            for (uintE i = 0; i < out_degree; i++)
            {
                uintV v = g.vertices_[u].getOutNeighbor(i);
                pr_next[v] += (pr_curr[u] / out_degree);
            }
        }

        // --- synchronization phase 1 start ---
        timer1.start();
        for (int i = 0; i < world_size; i++)
        {
            MPI_Reduce(pr_next + startVector[i], pr_recv, endVector[i] - startVector[i], PAGERANK_MPI_TYPE, MPI_SUM, i, MPI_COMM_WORLD);
        }
        communicate_time += timer1.stop();
        // --- synchronization phase 1 end -----

        for (int i = startVector[global_rank]; i < endVector[global_rank]; i++)
        {
            pr_next[i] = pr_recv[i - startVector[global_rank]];
        }

        // Loop 2
        for (uintV v = startVector[global_rank]; v < endVector[global_rank]; v++)
        {
            PageRankType new_value = PAGE_RANK(pr_next[v]);
            pr_curr[v] = new_value;
        }

        // Reset next_page_rank[v] to 0 for all vertices
        for (uintV v = 0; v < n; v++)
        {
            pr_next[v] = 0.0;
            pr_recv[v] = 0.0;
        }
    }

    PageRankType local_sum = 0;

    // loop 3
    for (uintV v = startVector[global_rank]; v < endVector[global_rank]; v++)
    {
        local_sum += pr_curr[v];
    }

    // --- synchronization phase 2 start ---
    PageRankType total_page_rank = 0;
    MPI_Reduce(&local_sum, &total_page_rank, 1, PAGERANK_MPI_TYPE, MPI_SUM, 0, MPI_COMM_WORLD);
    // --- synchronization phase 2 end --

    std::printf("%d, %u, %lf\n", global_rank, edges_processed, communicate_time);

    if (global_rank == 0)
    {
        time_taken = timer0.stop();
        std::printf("Sum of page rank : " PR_FMT "\n", total_page_rank);
        std::printf("Time taken (in seconds) : %f\n", time_taken);
    }

    delete[] startVector;
    delete[] endVector;
    delete[] pr_curr;
    delete[] pr_next;
    delete[] pr_recv;
}

int main(int argc, char *argv[])
{
    cxxopts::Options options("page_rank_push", "Calculate page_rank using serial and parallel execution");
    options.add_options("", {
                                {"nIterations", "Maximum number of iterations", cxxopts::value<uint>()->default_value(DEFAULT_MAX_ITER)},
                                {"strategy", "Strategy to be used", cxxopts::value<uint>()->default_value(DEFAULT_STRATEGY)},
                                {"inputFile", "Input graph file path", cxxopts::value<std::string>()->default_value("/scratch/input_graphs/roadNet-CA")},
                            });

    auto cl_options = options.parse(argc, argv);
    uint strategy = cl_options["strategy"].as<uint>();
    uint max_iterations = cl_options["nIterations"].as<uint>();
    std::string input_file_path = cl_options["inputFile"].as<std::string>();
    MPI_Init(NULL, NULL);

    int global_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &global_rank);
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
#ifdef USE_INT
    std::printf("Using INT\n");
#else
    std::printf("Using FLOAT\n");
#endif
    // Get the world size and print it out here
    if (global_rank == 0)
    {
        std::printf("World size : %d\n", world_size);
        std::printf("Communication strategy : %d\n", strategy);
        std::printf("Iterations : %d\n", max_iterations);
        std::printf("rank, num_edges, communication_time\n");
    }

    Graph g;
    g.readGraphFromBinary<int>(input_file_path);

    switch (strategy)
    {
    case 0:
        if (global_rank == 0)
        {
            pageRankSerial(g, max_iterations);
        }
        break;
    case 1:
        pageRankParallelStrategy1(g, max_iterations, global_rank, world_size);
        break;
    case 2:
        pageRankParallelStrategy2(g, max_iterations, global_rank, world_size);
        break;
    }
    MPI_Finalize();
    return 0;
}
