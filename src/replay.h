#pragma once

#include <string>

#include "metrics.h"

// Rebuilds <output_dir>/<input stem>_orderbook.csv from a market data CSV.
// No network is used.
bool replay_file(const std::string& path, const std::string& output_dir, Metrics& metrics);
