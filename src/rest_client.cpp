// This is the REST client behind --rest-snapshot. Every step is async on the io_context thread
// the WebSocket uses: resolve, connect, TLS handshake, write the GET, read the response.
// Connect, handshake, write and read each get 10 s.
//
// TLS is set up the same way as the WebSocket: SNI is set and the certificate has to match the
// host name. If setting SNI fails I post the failure instead of handling it right there, so
// done is never called from inside get(). Since each request has its own connection, after the
// response I just drop the socket, with no TLS shutdown. A non-200 answer is logged and handed
// back with an empty body, and a failure comes back as status 0. In finish() I move done_ out
// before calling it, because the callback may start the next request on this same client.
//
// stop() sets a flag and closes the socket, and every handler checks the flag first. The one
// thing it can't do is cancel a getaddrinfo that's already running, so if DNS stops answering
// in the middle of a round, shutdown can be delayed by up to ~30 s.

#include "rest_client.h"

#include <chrono>
#include <cstdio>
#include <utility>

#include <boost/asio/post.hpp>
#include <openssl/err.h>
#include <openssl/ssl.h>

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

RestClient::RestClient(net::io_context& ioc, ssl::context& ssl_ctx) : ioc_(ioc), ssl_ctx_(ssl_ctx), resolver_(ioc) {}

void RestClient::get(const std::string& host, const std::string& target, Done done) {
    if (stopped_) return;
    host_ = host;
    target_ = target;
    done_ = std::move(done);
    stream_.emplace(ioc_, ssl_ctx_);
    buffer_.consume(buffer_.size());

    if (!SSL_set_tlsext_host_name(stream_->native_handle(), host_.c_str())) {
        beast::error_code ec(static_cast<int>(::ERR_get_error()), net::error::get_ssl_category());
        net::post(ioc_, [this, ec] { fail("sni", ec); });
        return;
    }
    stream_->set_verify_callback(ssl::host_name_verification(host_));

    resolver_.async_resolve(host_, "443", [this](beast::error_code ec, tcp::resolver::results_type results) {
        on_resolve(ec, results);
    });
}

void RestClient::on_resolve(beast::error_code ec, tcp::resolver::results_type results) {
    if (stopped_) return;
    if (ec) return fail("resolve", ec);
    beast::get_lowest_layer(*stream_).expires_after(std::chrono::seconds(10));
    beast::get_lowest_layer(*stream_).async_connect(
        results, [this](beast::error_code ec2, const tcp::endpoint&) { on_connect(ec2); });
}

void RestClient::on_connect(beast::error_code ec) {
    if (stopped_) return;
    if (ec) return fail("connect", ec);
    beast::get_lowest_layer(*stream_).expires_after(std::chrono::seconds(10));
    stream_->async_handshake(ssl::stream_base::client, [this](beast::error_code ec2) { on_handshake(ec2); });
}

void RestClient::on_handshake(beast::error_code ec) {
    if (stopped_) return;
    if (ec) return fail("tls handshake", ec);
    req_ = Request(http::verb::get, target_, 11);
    req_.set(http::field::host, host_);
    req_.set(http::field::user_agent, "binance-lob-capture/1.0");
    beast::get_lowest_layer(*stream_).expires_after(std::chrono::seconds(10));
    http::async_write(*stream_, req_, [this](beast::error_code ec2, std::size_t) { on_write(ec2); });
}

void RestClient::on_write(beast::error_code ec) {
    if (stopped_) return;
    if (ec) return fail("write", ec);
    res_ = Response();
    beast::get_lowest_layer(*stream_).expires_after(std::chrono::seconds(10));
    http::async_read(*stream_, buffer_, res_, [this](beast::error_code ec2, std::size_t) { on_read(ec2); });
}

void RestClient::on_read(beast::error_code ec) {
    if (stopped_) return;
    if (ec) return fail("read", ec);
    close_socket();
    int status = static_cast<int>(res_.result_int());
    if (status != 200) {
        std::fprintf(stderr, "[rest] HTTP %d for %s\n", status, target_.c_str());
        return finish(status, {});
    }
    finish(status, std::move(res_.body()));
}

void RestClient::fail(const char* what, beast::error_code ec) {
    if (stopped_) return;
    std::fprintf(stderr, "[rest] %s failed: %s\n", what, ec.message().c_str());
    close_socket();
    finish(0, {});
}

void RestClient::finish(int status, std::string body) {
    Done done = std::move(done_);
    done_ = nullptr;
    if (done) done(status, std::move(body));
}

void RestClient::stop() {
    stopped_ = true;
    resolver_.cancel();
    close_socket();
}

void RestClient::close_socket() {
    if (!stream_) return;
    beast::error_code ignored;
    beast::get_lowest_layer(*stream_).socket().shutdown(tcp::socket::shutdown_both, ignored);
    beast::get_lowest_layer(*stream_).socket().close(ignored);
}
