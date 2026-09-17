#ifndef HELPER
#define HELPER
#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
std::vector<double> deserialize(const std::string &msg) {
  std::vector<double> res;
  std::stringstream ss(msg);
  double val;
  while (ss >> val) {
    res.push_back(val);
  }
  return res;
}
std::string serialize(const std::vector<double> &vec) {
  std::ostringstream ss;
  ss.precision(13);
  for (int i = 0; i < vec.size(); i++) {
    ss << vec[i];
    if (i + 1 < vec.size())
      ss << ' ';
  }
  return ss.str();
}
std::vector<std::vector<double>> getbatch(std::string &filename, int start_line,
                                          int end_line) {
  std::vector<std::vector<double>> batch;
  std::ifstream file(filename);
  if (!file.is_open()) {
    return batch; // empty
  }

  std::string line;
  int current = 1;

  while (std::getline(file, line)) {
    if (current > end_line)
      break;

    if (current >= start_line) {
      std::stringstream ss(line);
      double value;
      std::vector<double> row;

      // extract doubles from the line
      while (ss >> value) {
        row.push_back(value);
      }
      if (row.size() != 0)
        batch.push_back(row);
    }

    current++;
  }

  return batch;
}
#endif // !DEBUG
