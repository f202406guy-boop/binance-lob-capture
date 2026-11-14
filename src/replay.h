// This is the entry point for replay mode. replay_file() reads one market data CSV and
// rebuilds <output_dir>/<input file name minus .csv>_orderbook.csv from it, with no network
// at all. main.cpp calls it once for each file given to --replay. It returns false if the
// file can't be read properly (can't open it, wrong header, a quote that never closes) or the
// output file can't be created. The counters go into the Metrics you pass in, same as live.

#pragma once

#include <string>

#include "metrics.h"

bool replay_file(const std::string& path, const std::string& output_dir, Metrics& metrics);
