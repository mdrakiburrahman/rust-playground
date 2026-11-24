#include "contract.hpp"
#include <algorithm>
#include <chrono>
#include <csignal>
#include <iostream>
#include <thread>

namespace { volatile std::sig_atomic_t stopped = 0; void stop(int) { stopped = 1; } }

int main(int argc, char** argv) {
    try {
        std::string endpoint = "http://127.0.0.1:18830";
        int seconds = 300, interval_ms = 1000;
        for (int i = 1; i < argc; i += 2) {
            if (i + 1 == argc) throw std::invalid_argument("Each option requires a value");
            const std::string flag = argv[i], value = argv[i + 1];
            std::size_t consumed = 0;
            if (flag == "--endpoint") { endpoint = value; continue; }
            const auto number = std::stoi(value, &consumed);
            if (consumed != value.size()) throw std::invalid_argument("Expected an integer");
            if (flag == "--duration-seconds") seconds = number;
            else if (flag == "--interval-ms") interval_ms = number;
            else throw std::invalid_argument("Unknown option: " + flag);
        }
        if (seconds < 1 || seconds > 86400 || interval_ms < 1 || interval_ms > 60000)
            throw std::invalid_argument("Duration must be 1..86400 seconds; interval 1..60000 ms");
        std::signal(SIGINT, stop);
        auto stream = arrowcol_generated::EventsStream::Connect(endpoint);
        const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
        std::int64_t id = 0;
        while (!stopped && std::chrono::steady_clock::now() < end) {
            // Edit these values. This is the ordinary protoc-generated event class.
            arrowcol_generated::EventsContract::Event event;
            event.set_event_id(id);
            event.set_observed_at(1700000000000000 + id);
            event.set_payload("cpp-event-" + std::to_string(id));
            stream.Push(event);
            ++id;
            std::this_thread::sleep_until(std::min(end, std::chrono::steady_clock::now() + std::chrono::milliseconds(interval_ms)));
        }
        stream.Flush();
        stream.Close();
        std::cout << "{\"language\":\"cpp\",\"admitted_rows\":" << id << ",\"sink_commit_verified\":false}\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
