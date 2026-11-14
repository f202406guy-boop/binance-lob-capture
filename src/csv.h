#pragma once

#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>
#include <vector>

// RFC 4180: quote the field if it has a comma, quote or newline, and double
// any quote inside it.
void append_csv_field(std::string& out, std::string_view field);
void append_int(std::string& out, std::int64_t v);
void append_uint(std::string& out, std::uint64_t v);

// Splits one CSV record. Returns false if the text ends inside a quoted field,
// so the caller can append the next line and try again.
bool split_csv_row(std::string_view line, std::vector<std::string>& fields);

// Owns a FILE* and collects rows in memory, so we hit the kernel once per
// ~256 KB instead of once per row.
class BufferedFile {
public:
    BufferedFile() = default;
    ~BufferedFile();
    BufferedFile(const BufferedFile&) = delete;
    BufferedFile& operator=(const BufferedFile&) = delete;

    bool open(const std::string& path);
    bool is_open() const { return file_ != nullptr; }
    void write(std::string_view data);
    bool flush();
    void close();
    std::uint64_t write_errors() const { return write_errors_; }

private:
    static constexpr std::size_t kFlushThreshold = 256 * 1024;

    std::FILE* file_ = nullptr;
    std::string buffer_;
    std::uint64_t write_errors_ = 0;
};
