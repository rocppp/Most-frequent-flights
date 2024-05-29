# Import necessary modules
import csv
from threading import Thread
from queue import Queue
from collections import defaultdict
from functools import reduce
import time

class MapReduce:
    def __init__(self, chunk_size):
        # Initialize the MapReduce instance, set the chunk size, and create a queue to store results.
        self.chunk_size = chunk_size
        self.queue = Queue()

    def split_data_into_chunks(self, data):
        # Split the input data into chunks of size chunk_size and return a list of these chunks.
        return [data[i:i + self.chunk_size] for i in range(0, len(data), self.chunk_size)]

    def mapper_function(self, chunk):
        # Receive a data chunk as input, count the number of flights for each passenger, and put the result into the queue.
        flight_counts = defaultdict(int)
        for row in chunk:
            passenger_id = row[0]
            flight_counts[passenger_id] += 1
        self.queue.put(flight_counts)

    def reducer_function(self, flight_counts, reduced_data):
        # Perform the reduction step by merging the flight counts from different mappers.
        for passenger_id, times in reduced_data.items():
            flight_counts[passenger_id] += times
        return flight_counts

    def run_map_reduce(self, chunks_data):
        threads = []
        # Iterate through the list of data chunks, create a thread for each data chunk to execute the mapper method.
        for chunk in chunks_data:
            thread = Thread(target=self.mapper_function, args=(chunk,))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        reduced_data = []

        # Retrieve results from the queue and store them in the reduced_data list.
        while not self.queue.empty():
            reduced_data.append(self.queue.get())

        # Use the reduce method to perform the reduction operation.
        final_result = reduce(self.reducer_function, reduced_data)

        # Find the passenger ID with the highest number of flights and its flight count.
        passenger_id, highest_flight_count = max(final_result.items(), key=lambda x: x[1])

        print(f"Passenger ID with the most frequent flights: {passenger_id}")
        print(f"The most frequent flights: {highest_flight_count}")

def read_passenger_data(file_path):
    # Open the CSV file at the specified path and read its contents into a list.
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        return list(reader)

def main():
    # Read passenger data file.
    passenger_data = read_passenger_data('D:\\master\\semester two\\Big Data and cloud computing\\NEWCoursework\\CloudComputing\AComp_Passenger_data_no_error.csv')
    chunk_sizes = [9, 10, 50, 100]  # Different chunk sizes
    execution_times = []
    for chunk_size in chunk_sizes:
        # For each different chunk size, execute the MapReduce program.
        map_reduce = MapReduce(chunk_size=chunk_size)
        chunks_data = map_reduce.split_data_into_chunks(passenger_data)

        # Record program execution time.
        start_time = time.time()
        map_reduce.run_map_reduce(chunks_data)
        end_time = time.time()

        execution_time = end_time - start_time
        execution_times.append((chunk_size, execution_time))

    # Find the chunk size corresponding to the shortest execution time.
    shortest_chunk_size, shortest_execution_time = min(execution_times, key=lambda x: x[1])

    print(f"Minimum Chunk size: {shortest_chunk_size}")
    print(f"Minimum execution time: {shortest_execution_time} seconds")

if __name__ == "__main__":
    main()
