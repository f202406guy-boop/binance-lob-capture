// This header declares RestClient, a small async HTTPS GET client. I only use it for the REST
// /depth snapshot behind --rest-snapshot. It runs on the same io_context thread as the
// WebSocket, so a slow request never blocks the socket read.
//
// Each client does one request at a time: call get(), wait for done, then call get() again.
// To have several requests in flight, WsClient just makes several clients (--rest-parallel,
// 1 to 8, default 4). Every request opens a new connection.
//
// done is called exactly once per get(), unless stop() was called first, with the HTTP status,
// or 0 if the request never got a response. Like WsClient, the handlers capture `this`, so the
// client has to outlive io_context::run().

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
