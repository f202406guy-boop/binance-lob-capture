#!/usr/bin/env bash
# I used this benchmark to pick the --rest-parallel default. It runs binance_capture live on
# spot with 20 symbols and --rest-snapshot for 40 s, four copies at once with N = 1, 2, 4 and 8,
# so all four see the same network. For each N it prints how many of the 20 snapshots arrived,
# the time from the first market data row (any symbol) to the last rest_snapshot row (any
# symbol), and how many REST errors the log shows. The awk part reads recv_tsec, recv_tnsec
# and stream_kind (columns 1, 2 and 4) and skips the header row of each file.
#
# I ran it twice: 6.703 to 7.226 s with N=1, 3.336 to 3.924 s with 2, 1.750 to 1.849 s with 4
# and 1.019 to 1.105 s with 8, with 20/20 snapshots and 0 REST errors every time. I went with 4
# because it's about 3.9x faster than one at a time. 8 only saves another ~0.75 s, but it
# doubles the TLS handshakes on the io thread that also reads the WebSocket, and the requests
# already in flight when a 429 comes back. The request weight of a round is the same for any N.
#
# Request weight: each copy costs 20 x 50 = 1,000, so all four use 4,000 of Binance's 6,000 a
# minute per IP. Wait at least 70 s before running it again.
#
# usage: bench/rest_parallel_bench.sh [path/to/binance_capture]   default ./build/binance_capture

set -euo pipefail
shopt -s nullglob

bin=${1:-./build/binance_capture}
symbols=BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT,ADAUSDT,DOGEUSDT,TRXUSDT,LINKUSDT,AVAXUSDT,DOTUSDT,LTCUSDT,BCHUSDT,UNIUSDT,ATOMUSDT,ETCUSDT,XLMUSDT,FILUSDT,APTUSDT,NEARUSDT
duration=40
runs=(1 2 4 8)

if [[ ! -x $bin ]]; then
    echo "binary not found: $bin" >&2
    exit 1
fi
IFS=, read -ra list <<<"$symbols"
total=${#list[@]}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

pids=()
for n in "${runs[@]}"; do
    mkdir "$work/n$n"
    "$bin" --venue spot --symbols "$symbols" --duration "$duration" --rest-snapshot --rest-parallel "$n" \
        --output-dir "$work/n$n" 2>"$work/n$n.log" &
    pids+=($!)
done
echo "running N = ${runs[*]} at the same time, $total symbols, $duration s ..."
failed=()
for i in "${!runs[@]}"; do
    wait "${pids[$i]}" || failed+=("${runs[$i]}")
done

printf '%-3s %-10s %-26s %s\n' N snapshots "first row -> last snapshot" "rest errors"
for n in "${runs[@]}"; do
    files=()
    for f in "$work/n$n"/market_data_*.csv; do
        [[ $f == *_orderbook.csv ]] || files+=("$f")
    done
    if [[ ${#files[@]} -eq 0 ]]; then
        printf '%-3s %s\n' "$n" "no market data file"
        continue
    fi
    errors=$(grep -c -e '^\[rest\] .* failed' -e '^\[rest\] HTTP' "$work/n$n.log" || true)
    awk -F, -v n="$n" -v total="$total" -v errors="$errors" '
        FNR == 1 { next }
        {
            t = $1 + $2 / 1e9
            if (!seen || t < first) { first = t; seen = 1 }
            if ($4 == "rest_snapshot") { snaps++; if (t > last) last = t }
        }
        END {
            span = snaps > 0 ? sprintf("%.3f s", last - first) : "-"
            printf "%-3s %-10s %-26s %s\n", n, (snaps + 0) "/" total, span, errors
        }' "${files[@]}"
done

for n in "${failed[@]}"; do
    echo "N=$n exited with an error, end of its log:" >&2
    tail -n 5 "$work/n$n.log" >&2
done
[[ ${#failed[@]} -eq 0 ]]
