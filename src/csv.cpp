#include "csv.h"

#include <charconv>

void append_csv_field(std::string& out, std::string_view field) {
    if (field.find_first_of(",\"\r\n") == std::string_view::npos) {
        out.append(field);
        return;
    }
    out.push_back('"');
    for (char c : field) {
        if (c == '"') out.push_back('"');
        out.push_back(c);
    }
    out.push_back('"');
}

void append_int(std::string& out, std::int64_t v) {
    char buf[24];
    auto res = std::to_chars(buf, buf + sizeof(buf), v);
    out.append(buf, res.ptr);
}

void append_uint(std::string& out, std::uint64_t v) {
    char buf[24];
    auto res = std::to_chars(buf, buf + sizeof(buf), v);
    out.append(buf, res.ptr);
}

bool split_csv_row(std::string_view line, std::vector<std::string>& fields) {
    fields.clear();
    std::string field;
    bool in_quotes = false;
    for (std::size_t i = 0; i < line.size(); ++i) {
        char c = line[i];
        if (in_quotes) {
            if (c == '"') {
                if (i + 1 < line.size() && line[i + 1] == '"') {
                    field.push_back('"');
                    ++i;
                } else {
                    in_quotes = false;
                }
            } else {
                field.push_back(c);
            }
        } else if (c == '"') {
            in_quotes = true;
        } else if (c == ',') {
            fields.push_back(std::move(field));
            field.clear();
        } else if (c != '\r') {
            field.push_back(c);
        }
    }
    if (in_quotes) return false;
    fields.push_back(std::move(field));
    return true;
}

BufferedFile::~BufferedFile() {
    close();
}

bool BufferedFile::open(const std::string& path) {
    close();
    file_ = std::fopen(path.c_str(), "wb");
    if (!file_) return false;
    std::setvbuf(file_, nullptr, _IONBF, 0);
    buffer_.reserve(kFlushThreshold + 64 * 1024);
    return true;
}

void BufferedFile::write(std::string_view data) {
    buffer_.append(data);
    if (buffer_.size() >= kFlushThreshold) flush();
}

bool BufferedFile::flush() {
    if (!file_ || buffer_.empty()) return true;
    std::size_t written = std::fwrite(buffer_.data(), 1, buffer_.size(), file_);
    bool ok = written == buffer_.size();
    if (!ok) ++write_errors_;
    buffer_.clear();
    return ok;
}

void BufferedFile::close() {
    if (!file_) return;
    flush();
    if (std::fclose(file_) != 0) ++write_errors_;
    file_ = nullptr;
}
