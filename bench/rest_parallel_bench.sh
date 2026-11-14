#!/usr/bin/env bash
# REST snapshot benchmark: how long until every book has its REST snapshot,
# with --rest-parallel 1, 2, 4 and 8?
#
# Runs binance_capture --venue spot --rest-snapshot on 20 symbols for 40 s,
# once for each N. The four runs go at the same time, so they see the same
# network. For each N it prints the time from the first market data row (any
# symbol) to the last rest_snapshot row (any symbol), how many of the 20
# snapshots arrived, and how many REST errors the log shows.
#
# Request weight: one run costs 20 x 50 = 1,000, all four 4,000 of Binance's
# 6,000 a minute per IP. Wait at least 70 s before running it again.
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
    # columns: 1 recv_tsec, 2 recv_tnsec, 4 stream_kind; row 1 of each file is the header
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
