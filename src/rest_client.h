#pragma once

#include <functional>
#include <optional>
#include <string>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>

// Async HTTPS GET on the io_context thread, used for the REST depth snapshot.
// One request at a time (wait for done before the next get), and every request
// opens a new connection.
//
// done is called exactly once per get(), unless stop() was called first, with
// the HTTP status (0 if the request never got a response). The client must
// outlive io_context::run(); handlers capture `this`.
class RestClient {
public:
    using Done = std::function<void(int status, std::string body)>;

    RestClient(boost::asio::io_context& ioc, boost::asio::ssl::context& ssl_ctx);

    void get(const std::string& host, const std::string& target, Done done);
    void stop();

private:
    using Stream = boost::beast::ssl_stream<boost::beast::tcp_stream>;
    using Request = boost::beast::http::request<boost::beast::http::empty_body>;
    using Response = boost::beast::http::response<boost::beast::http::string_body>;

    void on_resolve(boost::beast::error_code ec, boost::asio::ip::tcp::resolver::results_type results);
    void on_connect(boost::beast::error_code ec);
    void on_handshake(boost::beast::error_code ec);
    void on_write(boost::beast::error_code ec);
    void on_read(boost::beast::error_code ec);
    void fail(const char* what, boost::beast::error_code ec);
    void finish(int status, std::string body);
    void close_socket();

    boost::asio::io_context& ioc_;
    boost::asio::ssl::context& ssl_ctx_;
    boost::asio::ip::tcp::resolver resolver_;
    std::optional<Stream> stream_;
    boost::beast::flat_buffer buffer_;
    Request req_;
    Response res_;
    std::string host_;
    std::string target_;
    Done done_;
    bool stopped_ = false;
};
