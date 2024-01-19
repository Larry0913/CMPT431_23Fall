#include <iostream>
#include <cstdio>
#include "core/utils.h"
#include "core/graph.h"
#include <mpi.h>

long countTriangles(uintV *array1, uintE len1, uintV *array2, uintE len2,
                    uintV u, uintV v)
{
    uintE i = 0, j = 0; // indexes for array1 and array2
    long count = 0;

    if (u == v)
        return count;

    while ((i < len1) && (j < len2))
    {
        if (array1[i] == array2[j])
        {
            if ((array1[i] != u) && (array1[i] != v))
            {
                count++;
            }
            else
            {
                // triangle with self-referential edge -> ignore
            }
            i++;
            j++;
        }
        else if (array1[i] < array2[j])
        {
            i++;
        }
        else
        {
            j++;
        }
    }
    return count;
}

void triangleCountSerial(Graph &g)
{
    uintV n = g.n_;
    long triangle_count = 0;
    double time_taken;
    timer t1;
    t1.start();
    for (uintV u = 0; u < n; u++)
    {
        uintE out_degree = g.vertices_[u].getOutDegree();
        for (uintE i = 0; i < out_degree; i++)
        {
            uintV v = g.vertices_[u].getOutNeighbor(i);
            triangle_count += countTriangles(g.vertices_[u].getInNeighbors(),
                                             g.vertices_[u].getInDegree(),
                                             g.vertices_[v].getOutNeighbors(),
                                             g.vertices_[v].getOutDegree(),
                                             u,
                                             v);
        }
    }

    // For every thread, print out the following statistics:
    // rank, edges, triangle_count, communication_time
    // 0, 17248443, 144441858, 0.000074
    // 1, 17248443, 152103585, 0.000020
    // 2, 17248443, 225182666, 0.000034
    // 3, 17248444, 185596640, 0.000022

    time_taken = t1.stop();

    // Print out overall statistics
    std::printf("Number of triangles : %ld\n", triangle_count);
    std::printf("Number of unique triangles : %ld\n", triangle_count / 3);
    std::printf("Time taken (in seconds) : %f\n", time_taken);
}

void triangleCountParallelStrategy1(Graph &g, int global_rank, int world_size)
{
    uintV n = g.n_;
    uintE m = g.m_;
    int startCount = 0;
    int endCount = 0;

    long local_count = 0;
    uintE edges_processed = 0;
    long global_count = 0;
    long *recValue = NULL;

    double time_taken = 0.0;
    timer t0;
    double communication_time = 0.0;
    timer t1;

    if (global_rank == 0)
    {
        t0.start();
    }
    // Edge decomposition strategy for a graph with n vertices and m edges for P processes
    for (int i = 0; i < world_size; i++)
    {
        startCount = endCount;
        long edges_count = 0;
        while (endCount < n)
        {
            // add vertices until we reach m/P edges.
            edges_count += g.vertices_[endCount].getOutDegree();
            endCount += 1;
            if (edges_count >= m / world_size)
                break;
        }
        if (i == global_rank)
            break;
    }

    for (uintV u = startCount; u < endCount; u++)
    {
        uintE out_degree = g.vertices_[u].getOutDegree();
        edges_processed += out_degree;
        for (uintE i = 0; i < out_degree; i++)
        {
            uintV v = g.vertices_[u].getOutNeighbor(i);
            local_count += countTriangles(g.vertices_[u].getInNeighbors(),
                                          g.vertices_[u].getInDegree(),
                                          g.vertices_[v].getOutNeighbors(),
                                          g.vertices_[v].getOutDegree(), u, v);
        }
    }

    if (global_rank == 0)
    {
        recValue = new long[world_size];
    }

    // --- synchronization phase start ---
    t1.start();
    if (global_rank != 0)
    {
        // depending on the strategy,
        // use appropriate API to send the local_count to the root process
        MPI_Gather(&local_count, 1, MPI_LONG, recValue, 1, MPI_LONG, 0, MPI_COMM_WORLD);
    }
    else
    {
        MPI_Gather(&local_count, 1, MPI_LONG, recValue, 1, MPI_LONG, 0, MPI_COMM_WORLD);
        for (int i = 0; i < world_size; i++)
        {
            global_count += recValue[i];
        }
    }

    communication_time = t1.stop();
    // --- synchronization phase end -----

    std::printf("%d, %u, %ld ,%lf\n", global_rank, edges_processed, local_count, communication_time);
    if (global_rank == 0)
    {
        // print process statistics and other results
        time_taken = t0.stop();
        std::printf("Number of triangles : %ld\n", global_count);
        std::printf("Number of unique triangles : %ld\n", global_count / 3);
        std::printf("Time taken (in seconds) : %f\n", time_taken);
    }
}

void triangleCountParallelStrategy2(Graph &g, int global_rank, int world_size)
{
    uintV n = g.n_;
    uintE m = g.m_;
    int startCount = 0;
    int endCount = 0;

    long local_count = 0;
    uintE edges_processed = 0;
    long global_count = 0;

    double time_taken = 0.0;
    timer t0;
    double communication_time = 0.0;
    timer t1;

    if (global_rank == 0)
    {
        t0.start();
    }
    // Edge decomposition strategy for a graph with n vertices and m edges for P processes
    for (int i = 0; i < world_size; i++)
    {
        startCount = endCount;
        long edges_count = 0;
        while (endCount < n)
        {
            // add vertices until we reach m/P edges.
            edges_count += g.vertices_[endCount].getOutDegree();
            endCount += 1;
            if (edges_count >= m / world_size)
                break;
        }
        if (i == global_rank)
            break;
    }

    for (uintV u = startCount; u < endCount; u++)
    {
        uintE out_degree = g.vertices_[u].getOutDegree();
        edges_processed += out_degree;
        for (uintE i = 0; i < out_degree; i++)
        {
            uintV v = g.vertices_[u].getOutNeighbor(i);
            local_count += countTriangles(g.vertices_[u].getInNeighbors(),
                                          g.vertices_[u].getInDegree(),
                                          g.vertices_[v].getOutNeighbors(),
                                          g.vertices_[v].getOutDegree(), u, v);
        }
    }

    // --- synchronization phase start ---
    t1.start();
    if (global_rank != 0)
    {
        // depending on the strategy,
        // use appropriate API to send the local_count to the root process
        MPI_Reduce(&local_count, &global_count, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    }
    else
    {
        MPI_Reduce(&local_count, &global_count, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    }

    communication_time = t1.stop();
    // --- synchronization phase end -----

    std::printf("%d, %u, %ld ,%lf\n", global_rank, edges_processed, local_count, communication_time);

    if (global_rank == 0)
    {
        time_taken = t0.stop();
        // print process statistics and other results
        std::printf("Number of triangles : %ld\n", global_count);
        std::printf("Number of unique triangles : %ld\n", global_count / 3);
        std::printf("Time taken (in seconds) : %f\n", time_taken);
    }
}

int main(int argc, char *argv[])
{
    cxxopts::Options options("triangle_counting_serial", "Count the number of triangles using serial and parallel execution");
    options.add_options("custom", {
                                      {"strategy", "Strategy to be used", cxxopts::value<uint>()->default_value(DEFAULT_STRATEGY)},
                                      {"inputFile", "Input graph file path", cxxopts::value<std::string>()->default_value("/scratch/input_graphs/roadNet-CA")},
                                  });

    auto cl_options = options.parse(argc, argv);
    uint strategy = cl_options["strategy"].as<uint>();
    std::string input_file_path = cl_options["inputFile"].as<std::string>();

    MPI_Init(NULL, NULL);

    int global_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &global_rank);
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    // Get the world size and print it out here
    if (global_rank == 0)
    {

        std::printf("World size : %d\n", world_size);
        std::printf("Communication strategy : %d\n", strategy);
        std::printf("rank, edges, triangle_count, communication_time\n");
    }

    Graph g;
    g.readGraphFromBinary<int>(input_file_path);

    switch (strategy)
    {
    case 0:
        if (global_rank == 0)
            triangleCountSerial(g);
        break;
    case 1:
        triangleCountParallelStrategy1(g, global_rank, world_size);
        break;
    case 2:
        triangleCountParallelStrategy2(g, global_rank, world_size);
        break;
    }
    MPI_Finalize();
    return 0;
}
