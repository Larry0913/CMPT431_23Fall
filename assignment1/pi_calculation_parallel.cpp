#include "core/utils.h"
#include <iomanip>
#include <iostream>
#include <stdlib.h>
#include <vector>
#include <thread>

#define sqr(x) ((x) * (x))
#define DEFAULT_NUMBER_OF_POINTS "12345678"

std::mutex mtx;
uint c_const = (uint)RAND_MAX + (uint)1;
uint total_circle_count = 0;
inline double get_random_coordinate(uint *random_seed)
{
    return ((double)rand_r(random_seed)) / c_const;
}

uint get_points_in_circle(uint n, uint random_seed)
{
    uint circle_count = 0;
    double x_coord, y_coord;
    for (uint i = 0; i < n; i++)
    {
        x_coord = (2.0 * get_random_coordinate(&random_seed)) - 1.0;
        y_coord = (2.0 * get_random_coordinate(&random_seed)) - 1.0;
        if ((sqr(x_coord) + sqr(y_coord)) <= 1.0)
            circle_count++;
    }
    return circle_count;
}

void piCalParallel(uint work_points, uint random_seed, int thread_id)
{
    timer timer_parallel;
    double time_taken = 0.0;
    timer_parallel.start();

    uint local_circle_count = get_points_in_circle(work_points, random_seed);
    time_taken = timer_parallel.stop();

    mtx.lock();
    std::cout << thread_id << ", " << work_points << ", " << local_circle_count << ", " << std::setprecision(TIME_PRECISION_PARALLEL) << time_taken << "\n";
    total_circle_count += local_circle_count;
    mtx.unlock();
}

void piCalculation(uint n, uint n_workers)
{
    timer parallel_timer;
    double time_taken = 0.0;
    uint random_seed = 1;

    parallel_timer.start();
    // Create threads and distribute the work across T threads
    // -------------------------------------------------------------------
    std::vector<std::thread> vector_threads;
    uint thread_points = n / n_workers;
    uint rest_points = n - ((n_workers - 1) * thread_points);
    uint work_points = thread_points;

    std::cout << "thread_id, points_generated, circle_points, time_taken\n";
    // Print the above statistics for each thread
    // Example output for 2 threads:
    // thread_id, points_generated, circle_points, time_taken
    // 1, 100, 90, 0.12
    // 0, 100, 89, 0.12

    for (int i = 0; i < n_workers; i++)
    {
        if (i == n_workers - 1)
        {
            work_points = rest_points;
        }
        vector_threads.push_back(std::thread(piCalParallel, work_points, random_seed, i));
        random_seed++;
    }

    for (int i = 0; i < n_workers; i++)
    {
        vector_threads[i].join();
    }

    // uint circle_points = get_points_in_circle(n, random_seed);
    double pi_value = 4.0 * (double)total_circle_count / (double)n;
    // -------------------------------------------------------------------
    time_taken = parallel_timer.stop();

    // Print the overall statistics
    std::cout << "Total points generated : " << n << "\n";
    std::cout << "Total points in circle : " << total_circle_count << "\n";
    std::cout << "Result : " << std::setprecision(VAL_PRECISION_PARALLEL) << pi_value
              << "\n";
    std::cout << "Time taken (in seconds) : " << std::setprecision(TIME_PRECISION_PARALLEL)
              << time_taken << "\n";
}

int main(int argc, char *argv[])
{
    // Initialize command line arguments
    cxxopts::Options options("pi_calculation",
                             "Calculate pi using serial and parallel execution");
    options.add_options(
        "custom",
        {
            {"nPoints", "Number of points",
             cxxopts::value<uint>()->default_value(DEFAULT_NUMBER_OF_POINTS)},
            {"nWorkers", "Number of workers",
             cxxopts::value<uint>()->default_value(DEFAULT_NUMBER_OF_WORKERS)},
        });

    auto cl_options = options.parse(argc, argv);
    uint n_points = cl_options["nPoints"].as<uint>();
    uint n_workers = cl_options["nWorkers"].as<uint>();
    std::cout << std::fixed;
    std::cout << "Number of points : " << n_points << "\n";
    std::cout << "Number of workers : " << n_workers << "\n";

    piCalculation(n_points, n_workers);

    return 0;
}
