// This header declares WsClient, the network side of the program. It keeps one combined-stream
// WebSocket connection for all symbols (shard 0), reads each frame, stamps the receive time,
// strips the {"stream":..,"data":..} envelope and pushes the payload into the SPSC queue that
// the book thread reads. The queue has 4096 slots, which is 20 to 80 s of data for one symbol.
//
// I wrote it with async Beast and it only ever runs on the one io_context thread, so there are
// no locks inside the client. The thing to be careful about is lifetime: handlers capture
// `this` and the connection epoch, so the client has to outlive io_context::run(). On any error
// it reconnects with backoff and bumps the epoch, and a late handler from the old connection
// sees a different epoch and just returns.
//
// With --rest-snapshot it also fetches every symbol's REST depth snapshot after each handshake
// and queues it as RestSnapshot. It owns one RestClient per request allowed in flight
// (--rest-parallel), rest_busy_ has one flag per RestClient, and symbols_ keeps the uppercase
// names because that's what the REST url wants. I ask for limit=1000 and not 5000, because on
// spot 5000 costs 250 request weight instead of 50, for levels the program never writes.

#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <simdjson.h>

#include "metrics.h"
#include "rest_client.h"
#include "spsc_queue.h"
#include "types.h"

constexpr std::size_t kQueueSize = 4096;
using MessageQueue = SpscQueue<RawMessage, kQueueSize>;

struct Endpoint {
    std::string host;
    std::string port;
    std::string target;
};

Endpoint make_endpoint(Venue venue, const std::vector<std::string>& symbols);

class WsClient {
public:
    WsClient(boost::asio::io_context& ioc, boost::asio::ssl::context& ssl_ctx, Venue venue,
             const std::vector<std::string>& symbols, MessageQueue& queue, Metrics& metrics, bool rest_snapshot,
             int rest_parallel);

    void start();
    void stop();

private:
    using Socket = boost::beast::websocket::stream<boost::beast::ssl_stream<boost::beast::tcp_stream>>;

    struct Route {
        std::uint32_t symbol_index;
        StreamKind kind;
    };

    void connect();
    void on_resolve(std::uint32_t epoch, boost::beast::error_code ec,
                    boost::asio::ip::tcp::resolver::results_type results);
    void on_connect(std::uint32_t epoch, boost::beast::error_code ec);
    void on_tls_handshake(std::uint32_t epoch, boost::beast::error_code ec);
    void on_ws_handshake(std::uint32_t epoch, boost::beast::error_code ec);
    void read();
    void on_read(std::uint32_t epoch, boost::beast::error_code ec);
    void fail(std::uint32_t epoch, const char* what, boost::beast::error_code ec);
    void handle_frame(std::int64_t recv_ns);
    void push(const Route& route, std::string_view data, std::int64_t recv_ns);
    void close_socket();
    void next_snapshot();
    void on_snapshot(std::size_t client, std::uint32_t epoch, std::uint32_t index, int status, std::string body);

    boost::asio::io_context& ioc_;
    boost::asio::ssl::context& ssl_ctx_;
    Endpoint endpoint_;
    std::string host_header_;
    MessageQueue& queue_;
    Metrics& metrics_;
    boost::asio::ip::tcp::resolver resolver_;
    boost::asio::steady_timer retry_timer_;
    boost::asio::steady_timer close_timer_;
    std::vector<std::unique_ptr<RestClient>> rest_;
    std::vector<bool> rest_busy_;
    std::vector<std::string> symbols_;
    std::string rest_host_;
    std::string rest_path_;
    std::optional<Socket> ws_;
    boost::beast::flat_buffer buffer_;
    std::map<std::string, Route, std::less<>> routes_;
    simdjson::ondemand::parser parser_;
    std::string frame_;
    std::uint32_t epoch_ = 0;
    std::uint64_t conn_seq_ = 0;
    std::int64_t recv_mono_ns_ = 0;
    int backoff_ms_ = 500;
    bool connected_ = false;
    bool stopping_ = false;
    bool rest_enabled_ = false;
    bool rest_started_ = false;
    std::uint32_t rest_next_ = 0;
    std::chrono::steady_clock::time_point last_rest_round_;
};
