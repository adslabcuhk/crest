#pragma once

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <ios>
#include <sstream>
#include <string>

#include "util/History.h"

#define ENABLE_PERF 1

using HistType = uint32_t;
using TickerType = uint32_t;

// THREAD-LOCAL statistics
class Statistics {
  struct TickerStruct {
    const std::string ticker_name;
    uint64_t ticker_value;

    TickerStruct(const std::string &name) : ticker_name(name), ticker_value(0) {}
  };

  struct HistStruct {
    const std::string hist_name;
    History hist;
    double scale;

    HistStruct(const std::string &name, double s) : hist_name(name), hist(), scale(s) {}
  };

 public:
  Statistics() : tickers_() {}

  ~Statistics() = default;

  Statistics(const Statistics &) = delete;
  Statistics &operator=(const Statistics &) = delete;

  void RegisterTicker(TickerType ticker_type, const std::string &ticker_name) {
    ASSERT(ticker_type == tickers_.size(), "");
    tickers_.emplace_back(ticker_name);
  }

  void RegisterHist(HistType hist_type, const std::string &hist_name, double scale = 1000.0) {
    ASSERT(hist_type == hists_.size(), "");
    hists_.emplace_back(hist_name, scale);
  }

  std::string ToString() const;

  void RecordTicker(TickerType ticker_type, uint64_t value) {
#if ENABLE_PERF
    tickers_[ticker_type].ticker_value += value;
#endif
  }

  void RecordHistory(HistType hist_type, uint64_t value) {
#if ENABLE_PERF
    hists_[hist_type].hist.Add(value);
#endif
  }

  // Get the information
  History *GetHistory(HistType hist_type) { return &(hists_[hist_type].hist); }

  uint64_t GetTicker(TickerType type) { return tickers_[type].ticker_value; }

  void Merge(const Statistics *stats) {
    for (int i = 0; i < tickers_.size(); ++i) {
      this->tickers_[i].ticker_value += stats->tickers_[i].ticker_value;
    }
    // Merge with another history
    for (int i = 0; i < hists_.size(); ++i) {
      this->hists_[i].hist.Merge(stats->hists_[i].hist);
    }
  }

 private:
  std::vector<TickerStruct> tickers_;
  std::vector<HistStruct> hists_;
};

// Wrapper functions

inline void RecordTicker(Statistics *stat, TickerType type, uint64_t value) {
#if ENABLE_PERF
  if (stat) [[likely]] {
    stat->RecordTicker(type, value);
  }
#endif
}

inline void RecordHistory(Statistics *stat, HistType type, uint64_t value) {
#if ENABLE_PERF
  if (stat) [[likely]] {
    stat->RecordHistory(type, value);
  }
#endif
}

inline std::string Statistics::ToString() const {
  const int MAX_HIST_NAME_SIZE =
      std::max_element(hists_.begin(), hists_.end(), [&](const auto &hist1, const auto &hist2) {
        return hist1.hist_name.size() < hist2.hist_name.size();
      })->hist_name.size();

  const int MAX_TICKER_NAME_SIZE =
      std::max_element(tickers_.begin(), tickers_.end(), [&](const auto &hist1, const auto &hist2) {
        return hist1.ticker_name.size() < hist2.ticker_name.size();
      })->ticker_name.size();

  std::stringstream ss;
  ss << "[Statistics]\n";

  ss << "[Ticker]\n";
  for (const auto &ticker : tickers_) {
    if (auto idx = ticker.ticker_name.find(".marker"); idx != std::string::npos) {
      // This is the start of a new class of history
      ss << "\n\n[ " << ticker.ticker_name.substr(0, idx) << " ]\n\n";
    } else {
      ss << std::setw(MAX_TICKER_NAME_SIZE) << ticker.ticker_name << ":";
      ss << std::fixed << std::setw(10) << ticker.ticker_value << "\n";
    }
  }
  ss << "[History]\n";

  for (const auto &hist : hists_) {
    if (auto idx = hist.hist_name.find(".marker"); idx != std::string::npos) {
      // This is the start of a new class of history
      ss << "\n\n[ " << hist.hist_name.substr(0, idx) << " ]\n\n";
    } else {
      ss << std::setw(MAX_HIST_NAME_SIZE) << hist.hist_name << ":";
      ss << "   average: " << std::fixed << std::setprecision(2) << std::setw(6)
         << hist.hist.Average() / hist.scale;
      ss << "   p50: " << std::fixed << std::setprecision(2) << std::setw(6)
         << hist.hist.Median() / hist.scale;
      ss << "   p99: " << std::fixed << std::setprecision(2) << std::setw(6)
         << hist.hist.Percentile(0.99) / hist.scale;
      ss << "   p999: " << std::fixed << std::setprecision(2) << std::setw(6)
         << hist.hist.Percentile(0.999) / hist.scale << "\n";
    }
  }

  return ss.str();
}