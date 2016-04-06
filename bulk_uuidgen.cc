// TODO: A better way to do this would be:
// (1) In stream mode: write to a preallocated buffer per
//     thread and pass the entire buffer to write(2) in
//     nonblocking mode. Access to write(2) must be synchronized.
// (2) In batch mode: mmap the batch files and serialize
//     directly into their buffers; don't even preallocate
//     a buffer.

#include <string>
#include <iostream>
#include <fstream>
#include <atomic>
#include <array>
#include <vector>
#include <thread>
#include <utility>

#include <softwear/concurrent_channel.hpp>

#include <boost/program_options.hpp>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <boost/algorithm/string/replace.hpp>

using std::string;
using std::atomic;
using std::cout;
using std::cerr;
using std::vector;
using std::array;
using std::thread;

string executable_name;

using boost::uuids::uuid;

using softwear::concurrent_channel;
using softwear::concurrent_channel_::producer_token;
using softwear::concurrent_channel_::consumer_token;

namespace po = boost::program_options;
po::options_description params("PARAMETERS");
po::positional_options_description args;
po::variables_map config;

//// implementation ////

void generator_thread(
    atomic<bool> &end
  , size_t batch_size
  , concurrent_channel<vector<uuid>> &queue) {

  cerr << "[INFO] Started generator\n";

  boost::uuids::random_generator gen;

  producer_token q_tok(queue);

  vector<uuid> batch(0);

  while (!end) {
    batch.resize(batch_size);

    for (auto &u : batch) u = gen();
    
    queue.enqueue(q_tok, std::move(batch));
  }

  cerr << "[INFO] Stopping generator\n";
}

void stdout_writer_thread(concurrent_channel<vector<uuid>> &queue) {

  cerr << "[INFO] Started writer\n";

  consumer_token q_tok(queue);

  vector<uuid> batch;

  while (cout && queue.dequeue(q_tok, batch)) {
    for (auto &u : batch)
      cout << u << '\n';
  }

  cerr << "[INFO] Stopped writer\n";
}

void batch_writer_thread(
    size_t total_size
  , string &out_pattern
  , atomic<size_t> &uuids_generated
  , concurrent_channel<vector<uuid>> &queue) {
  
  std::cerr << "[INFO] Started batch writer\n";

  consumer_token tok(queue);

  vector<uuid> batch(0);

  while (uuids_generated < total_size) {
    queue.dequeue(tok, batch);

    uuids_generated += batch.size();

    string path = boost::algorithm::replace_first_copy(
        out_pattern
      , "%%"
      , boost::uuids::to_string(batch[0]) );

    std::cerr << "[INFO] Writing batch " + path +  "\n";

    std::ofstream out(path);
    for (auto &u : batch)
      out << u << '\n';
  }

  std::cerr << "[INFO] Stopped batch writer\n";
}

void work(bool batch_mode) {
  size_t batch_size = config["batch_size"].as<size_t>();

  concurrent_channel<vector<uuid>> to_write;
  to_write.capacity_approx(200); // 200 batches

  atomic<bool> done{false};

  vector<thread> cpu_workers;
  for (size_t i=0; i < config["jobs"].as<size_t>(); i++)
    cpu_workers.emplace_back( [&done, &batch_size, &to_write]() {
      generator_thread(done, batch_size, to_write);
    });

  if (!batch_mode)
    stdout_writer_thread(to_write);

  else {
    size_t total_size = config["total"].as<size_t>();
    string out_pattern = config["out"].as<string>();

    atomic<size_t> uuids_generated(0);

    vector<thread> cpu_workers;
    auto startw = [batch_size, total_size, &out_pattern, &uuids_generated, &to_write]() {
      batch_writer_thread(
          total_size
        , out_pattern
        , uuids_generated
        , to_write);
    };

    vector<thread> io_workers;
    for (size_t i=0; i < config["iojobs"].as<size_t>(); i++)
      io_workers.emplace_back( startw );

    for (auto &w : io_workers) w.join();
  }

  // Killing the generators ungraciously
  cerr << "Done Generating UUIDs. Bye.\n";
  exit(0);
}

//// main ////

void usage() {
  string &ex = executable_name;
  std::cerr
    << "NAME"
    << "\n  fox_uuidpool – Create a pool of UUIDs for foxpro."
    << "\n\nSYNOPSIS"
    << "\n  (1) " << ex << " -h|--help"
    << "\n  (2) " << ex << " [-j|--jobs THREAD_NO] > uuid_list"
    << "\n  (3) " << ex << " [-j|--jobs THREAD_NO] [-i|--iojobs CONCURRENT_WRITES] TOTAL_UUIDS PER_FILE OUTPUT_FILE_PAT"
    << "\n\nDESCRIPTION"
    << "\n  (1) Print this help screen."
    << "\n  (2) Output UUIDs to stdout until stdout is closed."
    << "\n  (3) Output a fixed number of UUIDs, writing them into multiple files containing the specified amount of UUIDs. The output file pattern must contain '%%', which will be replaced with the current unix time at nanosecond precision."
    << "\n\n" << params << "\n";

  exit(1);
}

int main(int argc, const char **argv) {
  params.add_options()
    ("help,h", "Print a help message")
    ("jobs,j", po::value<size_t>()->default_value(2),
      "The number cpu of jobs to start")
    ("iojobs,i", po::value<size_t>()->default_value(8),
      "The number of files to write into concurrently")
    ("total", po::value<size_t>(),
      "Total number of UUIDs to generate")
    ("batch_size", po::value<size_t>()->default_value(10000),
      "Number of UUIDs to save in each file")
    ("out", po::value<string>(),
      "Where to save the UUIDs. Path must contain '%%', "
      "which will be replaced with a uuid time.");

  args.add("total", 1);
  args.add("batch_size", 1);
  args.add("out", 1);

  try {
    po::store(
        po::command_line_parser(argc, argv)
          .options(params)
          .positional(args)
          .run()
      , config);
    po::notify(config);
  } catch(po::error &e) {
    std::cerr << "Failed to parse the options: "
      << e.what() << "\n\n";
    usage();
  }

  auto c = [](const std::string &s){ return config.count(s); };

  if ( c("help") )
    usage();
  else if((config["jobs"].as<size_t>() < 1) || (config["iojobs"].as<size_t>() < 1))
    usage();
  else if ( c("total") && c("out"))
    work(true);
  else if ( !c("total") && !c("out"))
    work(false);
  else
    usage();
}
