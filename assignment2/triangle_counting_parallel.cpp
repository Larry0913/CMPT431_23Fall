#include "core/graph.h"
#include "core/utils.h"
#include <iomanip>
#include <iostream>
#include <stdlib.h>
#include <thread>

Graph g;
std::atomic<long> triangle_total_count(0);
std::atomic<uintV> current_vertex(0);

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
    double time_taken = 0.0;
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
                                             g.vertices_[v].getOutDegree(), u, v);
        }
    }
    time_taken = t1.stop();
    std::cout << "Number of triangles : " << triangle_count << "\n";
    std::cout << "Number of unique triangles : " << triangle_count / 3 << "\n";
    std::cout << "Time taken (in seconds) : " << std::setprecision(TIME_PRECISION)
              << time_taken << "\n";
}

void triCalParallel1(uintV start, uintV end, int thread_id)
{
    timer t1;
    double time_taken = 0.0;
    long triangle_local_count = 0;
    uintE num_edges = 0;

    t1.start();

    for (uintV u = start; u < end; u++)
    {
        // For each outNeighbor v, find the intersection of inNeighbor(u) and
        // outNeighbor(v)
        uintE out_degree = g.vertices_[u].getOutDegree();
        num_edges += out_degree;
        // std::cout << "out_degree is:" <<  out_degree << "\n";
        for (uintE i = 0; i < out_degree; i++)
        {
            uintV v = g.vertices_[u].getOutNeighbor(i);
            triangle_local_count += countTriangles(g.vertices_[u].getInNeighbors(),
                                                   g.vertices_[u].getInDegree(),
                                                   g.vertices_[v].getOutNeighbors(),
                                                   g.vertices_[v].getOutDegree(), u, v);
        }
    }

    time_taken = t1.stop();
    triangle_total_count += triangle_local_count;
    std::cout << thread_id << ", " << end - start << ", " << num_edges << ", " << triangle_local_count << ", " << time_taken << "\n";
}

void triangleCounting1(Graph &g, uint n_workers)
{
    uintV n = g.n_;
    double time_taken = 0.0;
    timer t1;
    double partition_time = 0.0;
    timer t2;
    t1.start();
    // Process each edge <u,v>
    std::cout << "thread_id, num_vertices, num_edges, triangle_count, time_taken\n";
    std::vector<std::thread> vector_threads;
    uint thread_points = n / n_workers;
    uintV start = 0;
    uintV end = 0;

    for (int i = 0; i < n_workers; i++)
    {
        t2.start();
        start = i * thread_points;
        end = (i + 1) * thread_points;
        if (i == n_workers - 1)
        {
            end = n;
        }
        // std::cout << "start: " << start << "    end: " << end << std::endl;
        partition_time += t2.stop();
        vector_threads.push_back(std::thread(triCalParallel1, start, end, i));
    }

    for (int i = 0; i < n_workers; i++)
    {
        vector_threads[i].join();
    }

    time_taken = t1.stop();

    // Print the overall statistics
    std::cout << "Number of triangles : " << triangle_total_count << "\n";
    std::cout << "Number of unique triangles : " << triangle_total_count / 3 << "\n";
    std::cout << "Partitioning time (in seconds) : " << std::setprecision(6) << partition_time << "\n";
    std::cout << "Time taken (in seconds) : " << std::setprecision(6)
              << time_taken << "\n";
}

void triCalParallel2(uintV start, uintV end, int thread_id)
{
    timer t1;
    double time_taken = 0.0;
    long triangle_local_count = 0;
    uintE num_edges = 0;

    t1.start();

    for (uintV u = start; u < end; u++)
    {
        // For each outNeighbor v, find the intersection of inNeighbor(u) and
        // outNeighbor(v)
        uintE out_degree = g.vertices_[u].getOutDegree();
        num_edges += out_degree;
        // std::cout << "out_degree is:" <<  out_degree << "\n";
        for (uintE i = 0; i < out_degree; i++)
        {
            uintV v = g.vertices_[u].getOutNeighbor(i);
            triangle_local_count += countTriangles(g.vertices_[u].getInNeighbors(),
                                                   g.vertices_[u].getInDegree(),
                                                   g.vertices_[v].getOutNeighbors(),
                                                   g.vertices_[v].getOutDegree(), u, v);
        }
    }

    time_taken = t1.stop();
    triangle_total_count += triangle_local_count;
    printf("%d, 0, %d, %ld, %lf\n", thread_id, num_edges, triangle_local_count, time_taken);
    // std::cout << thread_id << ", " << 0 << ", " << num_edges << ", " << triangle_local_count << ", " << time_taken << "\n";
}

void triangleCounting2(Graph &g, uint n_workers)
{
    uintV n = g.n_;
    uintE m = g.m_;
    double time_taken = 0.0;
    timer t1;
    double partition_time = 0.0;
    timer t2;
    t1.start();
    // Process each edge <u,v>
    std::cout << "thread_id, num_vertices, num_edges, triangle_count, time_taken\n";
    std::vector<std::thread> vector_threads;
    uint thread_edges_threashold = m / n_workers;
    uintV start = 0;
    uintV end = 0;
    int thread_id = 0;

    uintE edge_base = 0;
    for (int i = 0; i < n; i++)
    {
        t2.start();
        if (edge_base <= thread_edges_threashold)
        {
            edge_base += g.vertices_[i].getOutDegree();
        }
        else
        {
            end = i - 1;
            partition_time += t2.stop();
            vector_threads.push_back(std::thread(triCalParallel2, start, end, thread_id));
            start = end;
            edge_base = 0;
            thread_id++;
        }
        if (thread_id == n_workers - 1)
        {
            end = n;
            partition_time += t2.stop();
            vector_threads.push_back(std::thread(triCalParallel2, start, end, thread_id));
            break;
        }
    }

    for (int i = 0; i < n_workers; i++)
    {
        vector_threads[i].join();
    }

    time_taken = t1.stop();

    // Print the overall statistics
    std::cout << "Number of triangles : " << triangle_total_count << "\n";
    std::cout << "Number of unique triangles : " << triangle_total_count / 3 << "\n";
    std::cout << "Partitioning time (in seconds) : " << std::setprecision(6) << partition_time << "\n";
    std::cout << "Time taken (in seconds) : " << std::setprecision(6)
              << time_taken << "\n";
}

uintV getNextVertexToBeProcessed(uintV n)
{
    uintV current = current_vertex.fetch_add(1);
    if (current >= n)
        return -1;
    else
        return current;
}

void triCalParallel3(int thread_id)
{
    timer t1;
    uintV n = g.n_;
    double time_taken = 0.0;
    long triangle_local_count = 0;
    uintE num_edges = 0;
    uintV num_vertices = 0;

    t1.start();

    while (true)
    {
        uintV u = getNextVertexToBeProcessed(n);
        if (u == -1)
        {
            break;
        }

        uintE out_degree = g.vertices_[u].getOutDegree();
        num_edges += out_degree;
        // std::cout << "out_degree is:" <<  out_degree << "\n";
        for (uintE i = 0; i < out_degree; i++)
        {
            uintV v = g.vertices_[u].getOutNeighbor(i);
            triangle_local_count += countTriangles(g.vertices_[u].getInNeighbors(),
                                                   g.vertices_[u].getInDegree(),
                                                   g.vertices_[v].getOutNeighbors(),
                                                   g.vertices_[v].getOutDegree(), u, v);
        }
        num_vertices++;
    }

    time_taken = t1.stop();
    triangle_total_count += triangle_local_count;
    printf("%d, %d, %d, %ld, %lf\n", thread_id, num_vertices, num_edges, triangle_local_count, time_taken);
    // std::cout << thread_id << ", " << 0 << ", " << num_edges << ", " << triangle_local_count << ", " << time_taken << "\n";
}

void triangleCounting3(Graph &g, uint n_workers)
{
    double time_taken = 0.0;
    timer t1;
    double partition_time = 0;
    std::vector<std::thread> vector_threads;
    printf("thread_id, num_vertices, num_edges, triangle_count, time_taken\n");
    for (int i = 0; i < n_workers; i++)
    {
        vector_threads.push_back(std::thread(triCalParallel3, i));
    }
    t1.start();

    for (int i = 0; i < n_workers; i++)
    {
        vector_threads[i].join();
    }

    time_taken = t1.stop();
    std::cout << "Number of triangles : " << triangle_total_count << "\n";
    std::cout << "Number of unique triangles : " << triangle_total_count / 3 << "\n";
    std::cout << "Partitioning time (in seconds) : " << 0 << "\n";
    std::cout << "Time taken (in seconds) : " << std::setprecision(6)
              << time_taken << "\n";
}

int main(int argc, char *argv[])
{
    cxxopts::Options options(
        "triangle_counting_serial",
        "Count the number of triangles using serial and parallel execution");
    options.add_options(
        "custom",
        {
            {"nWorkers", "Number of workers",
             cxxopts::value<uint>()->default_value(DEFAULT_NUMBER_OF_WORKERS)},
            {"strategy", "Strategy to be used",
             cxxopts::value<uint>()->default_value(DEFAULT_STRATEGY)},
            {"inputFile", "Input graph file path",
             cxxopts::value<std::string>()->default_value(
                 "/scratch/input_graphs/roadNet-CA")},
        });

    auto cl_options = options.parse(argc, argv);
    uint n_workers = cl_options["nWorkers"].as<uint>();
    uint strategy = cl_options["strategy"].as<uint>();
    std::string input_file_path = cl_options["inputFile"].as<std::string>();
    std::cout << std::fixed;
    std::cout << "Number of workers : " << n_workers << "\n";
    std::cout << "Task decomposition strategy : " << strategy << "\n";

    // Graph g;
    std::cout << "Reading graph\n";
    g.readGraphFromBinary<int>(input_file_path);
    std::cout << "Created graph\n";

    switch (strategy)
    {
    case 0:
        std::cout << "\nSerial\n";
        triangleCountSerial(g);
        break;
    case 1:
        std::cout << "\nVertex-based work partitioning\n";
        triangleCounting1(g, n_workers);
        break;
    case 2:
        std::cout << "\nEdge-based work partitioning\n";
        triangleCounting2(g, n_workers);
        break;
    case 3:
        std::cout << "\nDynamic task mapping\n";
        triangleCounting3(g, n_workers);
        break;
    default:
        break;
    }

    return 0;
}
