// This is the CSV part: writing fields with RFC 4180 quoting, turning numbers into text,
// splitting a row back into fields for replay, and BufferedFile for the output files. A field
// only gets quotes if it has a comma, quote or newline in it, and any quote inside is doubled.
// In practice that's just the payload_json column. split_csv_row() returns false when the
// text ends inside a quoted field, so the caller can glue on the next line and try again.
// BufferedFile owns its FILE* (closed in the destructor) and collects rows in memory, so it
// goes to the kernel about once per 256 KB instead of once per row. The book thread still
// does that fwrite itself. A separate writer thread (or io_uring) would take it off the
// book's path, but for one write every 256 KB I left that for later. The code is in csv.cpp.

#pragma once

#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>
#include <vector>

void append_csv_field(std::string& out, std::string_view field);
void append_int(std::string& out, std::int64_t v);
void append_uint(std::string& out, std::uint64_t v);

bool split_csv_row(std::string_view line, std::vector<std::string>& fields);

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
