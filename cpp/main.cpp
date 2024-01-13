#include <algorithm>
#include <charconv>
#include <fstream>
#include <iostream>
#include <map>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <marl/defer.h>
#include <marl/event.h>
#include <marl/scheduler.h>
#include <marl/ticket.h>
#include <marl/waitgroup.h>

struct measurement_t {
  float min;
  float max;
  double total;
  double count;
};

static std::pair<std::string_view, std::string_view> split_by(
  std::string_view string_to_split, std::string_view separator) {
  std::pair<std::string_view, std::string_view> split_strs;
  for (auto separator_position = string_to_split.find(separator);
       separator_position != std::string::npos;
       separator_position = string_to_split.find(separator)) {
    auto split_str = string_to_split.substr(0, separator_position);
    string_to_split = string_to_split.substr(
      separator_position + separator.size(), string_to_split.size());
    split_strs.first = split_str;
  }
  auto split_str = string_to_split.substr(0, string_to_split.size());
  split_strs.second = split_str;
  return split_strs;
}

static size_t process_chunk(
  std::span<const char> buffer,
  std::map<std::string, measurement_t, std::less<>>& stations) {
  std::string_view data(buffer.data(), buffer.size());
  for (auto next = data.find('\n'); next != std::string_view::npos;
       next = data.find('\n')) {
    const auto line = data.substr(0, next);
    constexpr std::string_view separator = ";";
    const auto station_temp = split_by(line, separator);
    float temperature = std::stof(std::string(station_temp.second));
    if (auto it = stations.find(station_temp.first); it != stations.end()) {
      it->second.min = std::min(it->second.min, temperature);
      it->second.max = std::max(it->second.max, temperature);
      it->second.total += temperature;
      it->second.count++;
    } else {
      stations.insert(
        {std::string(station_temp.first),
         {temperature, temperature, temperature, 1}});
    }
    // move to next line
    data.remove_prefix(next + 1);
  }
  return data.size();
}

int main() {
  std::ifstream file("../measurements.txt", std::ios::binary);

  if (!file) {
    std::cerr << "File could not be opened!\n";
    return 1;
  }

  const size_t buffer_size = 1024; // 1MB
  std::vector<char> buffer(buffer_size);

  size_t offset = 0;
  std::map<std::string, measurement_t, std::less<>> all_stations;
  while (file) {
    file.read(buffer.data() + offset, buffer.size() - offset);
    std::streamsize bytes_read_count = file.gcount();
    // resize buffer to actual data size
    buffer.resize(bytes_read_count + offset);
    offset = process_chunk(buffer, all_stations);
    std::copy(buffer.end() - offset, buffer.end(), buffer.begin());
    // resize buffer back to original size for next read
    buffer.resize(buffer_size);
  }

  const auto rounded = [](const double value) {
    return std::copysign(std::round(std::abs(value) * 10) / 10, value);
  };

  // output

  std::cout << std::fixed;
  std::cout << std::setprecision(1);
  std::cout << "{";
  auto it = all_stations.begin();
  for (; it != std::prev(all_stations.end()); ++it) {
    const auto& [station, measurement] = *it;
    std::cout << station << '=' << measurement.min << '/'
              << rounded(measurement.total / (double)measurement.count) << '/'
              << measurement.max << ", ";
  }
  const auto& [station, measurement] = *it;
  std::cout << station << '=' << measurement.min << '/'
            << rounded(measurement.total / (double)measurement.count) << '/'
            << measurement.max;
  std::cout << "}\n";

  return 0;
}
