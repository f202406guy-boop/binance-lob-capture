// This is the network thread. Everything here runs on one io_context thread with async Beast:
// resolve, connect, TLS handshake, WebSocket handshake, then a read loop that stamps the
// receive time, looks up the stream name and copies the minified payload into a queue slot.
//
// Every connection has an epoch number and every handler carries the epoch it started with,
// so after a reconnect the old handlers see a different epoch and just return. On any error
// fail() closes the socket, bumps the epoch and retries after 0.5 s, doubling up to 30 s, and
// the wait goes back to 0.5 s after a good handshake. Binance needs SNI, so I set it, and the
// certificate has to match the host name. After the TLS handshake, Beast's idle timeout (20 s)
// and keep-alive pings turn a silent dead connection into a read error, so it reconnects.
//
// simdjson reads a bit past the end of its input, so each frame is copied into my own buffer
// that always has the padding. If the queue is full I wait, which pushes back on the socket,
// instead of dropping the message and leaving a hole in the market data file.
//
// The REST snapshots run on this same thread, so the WebSocket read is never blocked and only
// one thread ever pushes into the queue. I allow one round per 60 s at most, so a connection
// that keeps dropping can't burn through Binance's request weight. On HTTP 429 (too many
// requests) or 418 (IP banned) it stops asking on that connection and those books seed from
// depth5. Snapshots from an old connection are dropped (that RestClient sits out until its
// late answer comes in) and failed ones are skipped. On stop I send a polite close, 2 s max.

#include "ws_client.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <thread>
#include <utility>

#include <boost/asio/post.hpp>
#include <openssl/err.h>
#include <openssl/ssl.h>

namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

namespace {

constexpr std::size_t kMaxMessageBytes = 1 << 20;
constexpr int kMinBackoffMs = 500;
constexpr int kMaxBackoffMs = 30000;

std::string to_lower(std::string s) {
    for (char& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return s;
}

std::string to_upper(std::string s) {
    for (char& c : s) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return s;
}

const char* const kSuffixes[] = {"@depth@100ms", "@depth5@100ms", "@trade"};
const StreamKind kKinds[] = {StreamKind::DepthDiff, StreamKind::Depth5, StreamKind::Trade};

}

Endpoint make_endpoint(Venue venue, const std::vector<std::string>& symbols) {
    Endpoint e;
    if (venue == Venue::Spot) {
        e.host = "stream.binance.com";
        e.port = "9443";
        e.target = "/stream?streams=";
    } else {
        e.host = "fstream.binance.com";
        e.port = "443";
        e.target = "/public/stream?streams=";
    }
    bool first = true;
    for (const auto& sym : symbols) {
        std::string lower = to_lower(sym);
        for (const char* suffix : kSuffixes) {
            if (!first) e.target += '/';
            e.target += lower;
            e.target += suffix;
            first = false;
        }
    }
    return e;
}

WsClient::WsClient(net::io_context& ioc, ssl::context& ssl_ctx, Venue venue, const std::vector<std::string>& symbols,
                   MessageQueue& queue, Metrics& metrics, bool rest_snapshot, int rest_parallel)
    : ioc_(ioc),
      ssl_ctx_(ssl_ctx),
      endpoint_(make_endpoint(venue, symbols)),
      host_header_(endpoint_.host + ":" + endpoint_.port),
      queue_(queue),
      metrics_(metrics),
      resolver_(ioc),
      retry_timer_(ioc),
      close_timer_(ioc),
      rest_enabled_(rest_snapshot) {
    for (int i = 0; i < std::max(rest_parallel, 1); ++i) {
        rest_.push_back(std::make_unique<RestClient>(ioc, ssl_ctx));
        rest_busy_.push_back(false);
    }
    for (std::uint32_t i = 0; i < symbols.size(); ++i) {
        std::string lower = to_lower(symbols[i]);
        for (int k = 0; k < 3; ++k) routes_.emplace(lower + kSuffixes[k], Route{i, kKinds[k]});
        symbols_.push_back(to_upper(symbols[i]));
    }
    if (venue == Venue::Spot) {
        rest_host_ = "api.binance.com";
        rest_path_ = "/api/v3/depth";
    } else {
        rest_host_ = "fapi.binance.com";
        rest_path_ = "/fapi/v1/depth";
    }
    frame_.reserve(64 * 1024);
}

void WsClient::start() {
    connect();
}

void WsClient::connect() {
    if (stopping_) return;
    ws_.emplace(ioc_, ssl_ctx_);
    connected_ = false;

    if (!SSL_set_tlsext_host_name(ws_->next_layer().native_handle(), endpoint_.host.c_str())) {
        beast::error_code ec(static_cast<int>(::ERR_get_error()), net::error::get_ssl_category());
        fail(epoch_, "sni", ec);
        return;
    }
    ws_->next_layer().set_verify_callback(ssl::host_name_verification(endpoint_.host));

    std::uint32_t epoch = epoch_;
    resolver_.async_resolve(endpoint_.host, endpoint_.port,
                            [this, epoch](beast::error_code ec, tcp::resolver::results_type results) {
                                on_resolve(epoch, ec, results);
                            });
}

void WsClient::on_resolve(std::uint32_t epoch, beast::error_code ec, tcp::resolver::results_type results) {
    if (epoch != epoch_ || stopping_) return;
    if (ec) return fail(epoch, "resolve", ec);
    beast::get_lowest_layer(*ws_).expires_after(std::chrono::seconds(10));
    beast::get_lowest_layer(*ws_).async_connect(
        results, [this, epoch](beast::error_code ec2, const tcp::endpoint&) { on_connect(epoch, ec2); });
}

void WsClient::on_connect(std::uint32_t epoch, beast::error_code ec) {
    if (epoch != epoch_ || stopping_) return;
    if (ec) return fail(epoch, "connect", ec);
    beast::get_lowest_layer(*ws_).socket().set_option(tcp::no_delay(true), ec);
    beast::get_lowest_layer(*ws_).expires_after(std::chrono::seconds(10));
    ws_->next_layer().async_handshake(ssl::stream_base::client,
                                      [this, epoch](beast::error_code ec2) { on_tls_handshake(epoch, ec2); });
}

void WsClient::on_tls_handshake(std::uint32_t epoch, beast::error_code ec) {
    if (epoch != epoch_ || stopping_) return;
    if (ec) return fail(epoch, "tls handshake", ec);

    beast::get_lowest_layer(*ws_).expires_never();
    websocket::stream_base::timeout opt{};
    opt.handshake_timeout = std::chrono::seconds(10);
    opt.idle_timeout = std::chrono::seconds(20);
    opt.keep_alive_pings = true;
    ws_->set_option(opt);
    ws_->set_option(websocket::stream_base::decorator(
        [](websocket::request_type& req) { req.set(beast::http::field::user_agent, "binance-lob-capture/1.0"); }));
    ws_->read_message_max(kMaxMessageBytes);

    ws_->async_handshake(host_header_, endpoint_.target,
                         [this, epoch](beast::error_code ec2) { on_ws_handshake(epoch, ec2); });
}

void WsClient::on_ws_handshake(std::uint32_t epoch, beast::error_code ec) {
    if (epoch != epoch_ || stopping_) return;
    if (ec) return fail(epoch, "websocket handshake", ec);
    connected_ = true;
    backoff_ms_ = kMinBackoffMs;
    conn_seq_ = 0;
    std::fprintf(stderr, "[ws] connected epoch=%u wss://%s%s\n", epoch_, host_header_.c_str(),
                 endpoint_.target.c_str());
    read();
    if (rest_enabled_) {
        auto now = std::chrono::steady_clock::now();
        if (!rest_started_ || now - last_rest_round_ >= std::chrono::seconds(60)) {
            rest_started_ = true;
            last_rest_round_ = now;
            rest_next_ = 0;
            next_snapshot();
        } else {
            std::fprintf(stderr, "[rest] last snapshot round was less than 60 s ago, skipping this one\n");
        }
    }
}

void WsClient::next_snapshot() {
    for (std::size_t c = 0; c < rest_.size(); ++c) {
        if (stopping_ || !connected_ || rest_next_ >= symbols_.size()) return;
        if (rest_busy_[c]) continue;
        std::uint32_t epoch = epoch_;
        std::uint32_t index = rest_next_++;
        std::string target = rest_path_ + "?symbol=" + symbols_[index] + "&limit=1000";
        rest_busy_[c] = true;
        rest_[c]->get(rest_host_, target, [this, c, epoch, index](int status, std::string body) {
            on_snapshot(c, epoch, index, status, std::move(body));
        });
    }
}

void WsClient::on_snapshot(std::size_t client, std::uint32_t epoch, std::uint32_t index, int status,
                           std::string body) {
    rest_busy_[client] = false;
    if (stopping_) return;
    if (epoch == epoch_) {
        if (status == 200) {
            recv_mono_ns_ = mono_clock_ns();
            push(Route{index, StreamKind::RestSnapshot}, body, wall_clock_ns());
            std::fprintf(stderr, "[rest] %s snapshot %zu bytes (epoch=%u)\n", symbols_[index].c_str(), body.size(),
                         epoch);
        }
        if (status == 429 || status == 418) {
            std::fprintf(stderr, "[rest] rate limited (HTTP %d), no more snapshots on this connection\n", status);
            rest_next_ = static_cast<std::uint32_t>(symbols_.size());
        }
    }
    net::post(ioc_, [this] { next_snapshot(); });
}

void WsClient::read() {
    std::uint32_t epoch = epoch_;
    ws_->async_read(buffer_, [this, epoch](beast::error_code ec, std::size_t) { on_read(epoch, ec); });
}

void WsClient::on_read(std::uint32_t epoch, beast::error_code ec) {
    if (epoch != epoch_) return;
    if (ec) {
        if (stopping_) return;
        return fail(epoch, "read", ec);
    }
    recv_mono_ns_ = mono_clock_ns();
    std::int64_t recv_ns = wall_clock_ns();
    handle_frame(recv_ns);
    buffer_.consume(buffer_.size());
    if (!stopping_) read();
}

void WsClient::handle_frame(std::int64_t recv_ns) {
    bump(metrics_.messages);
    auto data = buffer_.data();
    frame_.assign(static_cast<const char*>(data.data()), data.size());
    if (frame_.capacity() - frame_.size() < simdjson::SIMDJSON_PADDING)
        frame_.reserve(frame_.size() + simdjson::SIMDJSON_PADDING);

    try {
        simdjson::ondemand::document doc =
            parser_.iterate(simdjson::padded_string_view(frame_.data(), frame_.size(), frame_.capacity()));
        std::string_view stream = doc["stream"].get_string();
        std::string_view payload = doc["data"].get_object().raw_json();
        auto it = routes_.find(stream);
        if (it == routes_.end()) {
            bump(metrics_.unknown_streams);
            return;
        }
        push(it->second, payload, recv_ns);
    } catch (const simdjson::simdjson_error& e) {
        std::uint64_t n = metrics_.parse_errors.fetch_add(1, std::memory_order_relaxed) + 1;
        if (n <= 10) std::fprintf(stderr, "[ws] bad frame: %s\n", e.what());
    }
}

void WsClient::push(const Route& route, std::string_view data, std::int64_t recv_ns) {
    RawMessage* slot = queue_.begin_push();
    while (slot == nullptr) {
        bump(metrics_.queue_full_waits);
        std::this_thread::sleep_for(std::chrono::microseconds(50));
        slot = queue_.begin_push();
    }

    slot->recv_ns = recv_ns;
    slot->recv_mono_ns = recv_mono_ns_;
    slot->shard_id = 0;
    slot->conn_epoch = epoch_;
    slot->conn_seq = conn_seq_++;
    slot->symbol_index = route.symbol_index;
    slot->kind = route.kind;

    slot->payload.resize(data.size() + simdjson::SIMDJSON_PADDING);
    std::size_t out_len = 0;
    if (simdjson::minify(data.data(), data.size(), slot->payload.data(), out_len) == simdjson::SUCCESS)
        slot->payload.resize(out_len);
    else
        slot->payload.assign(data.data(), data.size());

    queue_.end_push();
}

void WsClient::fail(std::uint32_t epoch, const char* what, beast::error_code ec) {
    if (epoch != epoch_ || stopping_) return;
    std::fprintf(stderr, "[ws] %s failed (epoch=%u): %s\n", what, epoch, ec.message().c_str());
    close_socket();
    connected_ = false;

    ++epoch_;
    bump(metrics_.reconnects);
    int delay = backoff_ms_;
    backoff_ms_ = std::min(backoff_ms_ * 2, kMaxBackoffMs);
    std::fprintf(stderr, "[ws] reconnecting in %d ms as epoch %u\n", delay, epoch_);

    retry_timer_.expires_after(std::chrono::milliseconds(delay));
    retry_timer_.async_wait([this](beast::error_code tec) {
        if (tec || stopping_) return;
        connect();
    });
}

void WsClient::stop() {
    if (stopping_) return;
    stopping_ = true;
    retry_timer_.cancel();
    resolver_.cancel();
    for (auto& rest : rest_) rest->stop();
    if (!ws_) return;

    if (connected_ && ws_->is_open()) {
        close_timer_.expires_after(std::chrono::seconds(2));
        close_timer_.async_wait([this](beast::error_code) { close_socket(); });
        ws_->async_close(websocket::close_code::normal, [this](beast::error_code) {
            close_timer_.cancel();
            close_socket();
        });
    } else {
        close_socket();
    }
}

void WsClient::close_socket() {
    if (!ws_) return;
    beast::error_code ignored;
    beast::get_lowest_layer(*ws_).socket().shutdown(tcp::socket::shutdown_both, ignored);
    beast::get_lowest_layer(*ws_).socket().close(ignored);
}
