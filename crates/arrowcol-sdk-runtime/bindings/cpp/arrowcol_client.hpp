#pragma once
#include "arrowcol_stream.hpp"
#include <fstream>
#include <set>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>

extern "C" {
char* arrowcol_open(const char* endpoint, const char* contract_path,
    const std::uint8_t* model_descriptor, std::size_t model_descriptor_len,
    const char* model_full_name, const char* options_json, void** output);
char* arrowcol_push(void*, const std::uint8_t*, std::size_t, std::uint8_t);
char* arrowcol_flush(void*);
char* arrowcol_close(void*);
void arrowcol_destroy(void*);
void arrowcol_free_error(char*);
}

namespace arrowcol {
class ArrowcolContract {
public:
    static ArrowcolContract Load(std::string path) {
        if (path.empty() || path.find('\0') != std::string::npos)
            throw std::invalid_argument("Invalid contract path");
        for (const auto* name : {"manifest.json", "schema.arrow", "descriptor.pb"}) {
            std::ifstream file(path + "/" + name, std::ios::binary);
            if (!file) throw std::invalid_argument(std::string("Missing contract artifact: ") + name);
        }
        return ArrowcolContract(std::move(path));
    }
    const std::string& Path() const noexcept { return path_; }
private:
    explicit ArrowcolContract(std::string path) : path_(std::move(path)) {}
    std::string path_;
};

namespace detail {
inline void AddDescriptors(const google::protobuf::FileDescriptor* file,
    google::protobuf::FileDescriptorSet& result, std::set<std::string>& seen) {
    if (!seen.insert(file->name()).second) return;
    for (int i = 0; i < file->dependency_count(); ++i)
        AddDescriptors(file->dependency(i), result, seen);
    file->CopyTo(result.add_file());
}
template<class T> struct NativeEvent {
    using Event = T;
    static char* Push(void* handle, const std::uint8_t* data, std::size_t length, std::uint8_t batch) {
        return arrowcol_push(handle, data, length, batch);
    }
    static char* Flush(void* handle) { return arrowcol_flush(handle); }
    static char* Close(void* handle) { return arrowcol_close(handle); }
    static void Destroy(void* handle) { arrowcol_destroy(handle); }
    static void FreeError(char* error) { arrowcol_free_error(error); }
};
}

// One shared native engine; Event is an ordinary protoc-generated protobuf class.
template<class Event> class ArrowcolStream {
    using Engine = GeneratedStream<detail::NativeEvent<Event>>;
public:
    static ArrowcolStream Connect(const std::string& endpoint, const ArrowcolContract& contract,
        StreamOptions options = {}) {
        if (endpoint.find('\0') != std::string::npos) throw std::invalid_argument("Endpoint contains NUL");
        const auto* descriptor = Event::descriptor();
        google::protobuf::FileDescriptorSet files;
        std::set<std::string> seen;
        detail::AddDescriptors(descriptor->file(), files, seen);
        std::string bytes;
        if (!files.SerializeToString(&bytes)) throw std::invalid_argument("Invalid compiled descriptors");
        void* handle = nullptr;
        // Native open compares the compiled descriptor before contacting the endpoint.
        char* error = arrowcol_open(endpoint.c_str(), contract.Path().c_str(),
            reinterpret_cast<const std::uint8_t*>(bytes.data()), bytes.size(),
            descriptor->full_name().c_str(), options.Json().c_str(), &handle);
        if (error) {
            std::unique_ptr<char, decltype(&arrowcol_free_error)> owned(error, &arrowcol_free_error);
            throw std::runtime_error(owned.get());
        }
        return ArrowcolStream(Engine::FromHandle(handle, options.batch_rows));
    }
    ArrowcolStream(const ArrowcolStream&) = delete;
    ArrowcolStream& operator=(const ArrowcolStream&) = delete;
    ArrowcolStream(ArrowcolStream&&) noexcept = default;
    ArrowcolStream& operator=(ArrowcolStream&&) noexcept = default;
    void Push(const Event& event) { engine_.Push(event); }
    void PushBatch(const std::vector<Event>& events) { engine_.PushBatch(events); }
    void Flush() { engine_.Flush(); }
    void Close() { engine_.Close(); }
private:
    explicit ArrowcolStream(Engine engine) : engine_(std::move(engine)) {}
    Engine engine_;
};
}
