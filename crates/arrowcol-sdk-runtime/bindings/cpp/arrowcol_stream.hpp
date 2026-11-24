#pragma once
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace arrowcol {
struct StreamOptions {
    std::size_t batch_rows = 1024;
    std::size_t batch_bytes = 1024 * 1024;
    std::size_t buffer_bytes = 32 * 1024 * 1024;
    std::uint64_t flush_interval_ms = 1000;
    std::uint64_t timeout_ms = 30000;
    std::string Json() const {
        std::ostringstream s;
        s << "{\"batch_rows\":" << batch_rows << ",\"batch_bytes\":" << batch_bytes
          << ",\"buffer_bytes\":" << buffer_bytes << ",\"flush_interval_ms\":" << flush_interval_ms
          << ",\"timeout_ms\":" << timeout_ms << "}";
        return s.str();
    }
};

// Contract is generated from the schema. This transport/lifecycle implementation is shared.
template<class Contract> class GeneratedStream {
public:
    using Event = typename Contract::Event;
    static GeneratedStream Connect(const std::string& endpoint, StreamOptions options = {}) {
        if (endpoint.find('\0') != std::string::npos) throw std::invalid_argument("Endpoint contains NUL");
        GeneratedStream stream(options.batch_rows);
        Check(Contract::Open(endpoint.c_str(), options.Json().c_str(), &stream.handle_));
        return stream;
    }
    static GeneratedStream FromHandle(void* handle, std::size_t max_rows) {
        GeneratedStream stream(max_rows);
        stream.handle_ = handle;
        return stream;
    }
    GeneratedStream(const GeneratedStream&) = delete;
    GeneratedStream& operator=(const GeneratedStream&) = delete;
    GeneratedStream(GeneratedStream&& other) noexcept
        : handle_(std::exchange(other.handle_, nullptr)), max_rows_(other.max_rows_) {}
    GeneratedStream& operator=(GeneratedStream&& other) noexcept {
        if (this != &other) {
            Contract::Destroy(handle_);
            handle_ = std::exchange(other.handle_, nullptr);
            max_rows_ = other.max_rows_;
        }
        return *this;
    }
    // Destruction frees resources only; explicit Close surfaces delivery errors.
    ~GeneratedStream() { Contract::Destroy(handle_); }
    void Push(const Event& event) {
        std::string bytes;
        if (!event.SerializeToString(&bytes)) throw std::invalid_argument("Invalid protobuf event");
        Check(Contract::Push(handle_, reinterpret_cast<const std::uint8_t*>(bytes.data()), bytes.size(), 0));
    }
    void PushBatch(const std::vector<Event>& events) {
        if (events.size() > max_rows_) throw std::invalid_argument("Batch exceeds batch_rows");
        std::string encoded;
        for (const auto& event : events) {
            std::string bytes;
            if (!event.SerializeToString(&bytes)) throw std::invalid_argument("Invalid protobuf event");
            if (encoded.size() + bytes.size() + 5 > 16 * 1024 * 1024)
                throw std::invalid_argument("Encoded batch exceeds 16 MiB");
            auto size = static_cast<std::uint32_t>(bytes.size());
            while (size >= 128) { encoded.push_back(static_cast<char>((size & 127) | 128)); size >>= 7; }
            encoded.push_back(static_cast<char>(size));
            encoded += bytes;
        }
        Check(Contract::Push(handle_, reinterpret_cast<const std::uint8_t*>(encoded.data()), encoded.size(), 1));
    }
    void Flush() { Check(Contract::Flush(handle_)); }
    void Close() { Check(Contract::Close(handle_)); }
private:
    explicit GeneratedStream(std::size_t max_rows) : max_rows_(max_rows) {}
    static void Check(char* error) {
        if (!error) return;
        std::unique_ptr<char, decltype(&Contract::FreeError)> owned(error, &Contract::FreeError);
        throw std::runtime_error(owned.get());
    }
    void* handle_ = nullptr;
    std::size_t max_rows_;
};
}
