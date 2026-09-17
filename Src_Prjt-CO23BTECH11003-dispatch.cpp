#include "ZmqNode.hpp"
#include "helper.hpp"
#include <cmath>
#include <cstdlib>
#include <stdexcept>
#include <string>

int main(int argc, char **argv)
{
  using namespace std;
  int nworkers = -1;
  string node_id = "d";
  string port;
  string log_file;
  string ip = "*";
  string wip = "localhost";
  string worker_prefix = "w";
  string data_file;
  int base_port = -1;
  int batch_size = 10;
  double lr = 1e-2;

  for (int i = 1; i < argc; i++)
  {
    string arg = argv[i];
    if (arg == "-n" && i + 1 < argc)
    {
      nworkers = atoi(argv[++i]);
    }
    else if (arg == "-id" && i + 1 < argc)
    {
      node_id = argv[++i];
    }
    else if (arg.rfind("--log=", 0) == 0)
    {
      log_file = arg.substr(6);
    }
    else if (arg == "-port" && i + 1 < argc)
    {
      port = argv[++i];
    }
    else if (arg.rfind("--ip=", 0) == 0)
    {
      ip = arg.substr(5);
    }
    else if (arg.rfind("--prefix=", 0) == 0)
    {
      worker_prefix = arg.substr(9);
    }
    else if (arg.rfind("--lr=", 0) == 0)
    {
      lr = stold(arg.substr(5));
    }
    else if (arg.rfind("--wip=", 0) == 0)
    {
      wip = arg.substr(6);
    }
    else if (arg.rfind("-base=", 0) == 0)
    {
      base_port = stoi(arg.substr(6));
    }
    else if (arg.rfind("-data=", 0) == 0)
    {
      data_file = arg.substr(6);
    }
    else
    {
      throw runtime_error("Unknown argument: " + arg + "\n");
    }
  }

  if (log_file.empty())
  {
    log_file = node_id + ".log";
  }

  if (nworkers <= 0 || port.empty() || base_port == -1 || data_file.empty())
  {
    throw runtime_error(
        "Incorrect/No numeber of workers or port or base port or data file "
        "mentioned, ./<executable> "
        "-n <number of workers>\n"
        "-id <name: def = d>\n"
        "--log=<log_file: def = <id>.log>\n"
        "-port <port_num>\n"
        "--ip=<ip address : def=localhost>\n"
        "--prefix=<worker_prefix: def = w>\n"
        "--lr=<learning rate: def=1e-2>\n"
        "--wip=<workde ip: def=localhost>\n"
        "-base=<worker base port>\n"
        "-data=<data_file>\n");
  }
  FILE *log = fopen(log_file.c_str(), "w");

  ZmqNode conn(node_id, ip, port);
  for (int i = 1; i <= nworkers; i++)
  {
    printf("%s\n", (wip + ":" + to_string(base_port + i)).c_str());
    conn.connect_to(wip + ":" + to_string(base_port + i),
                    worker_prefix + to_string(i));
  }
  int check = nworkers;

  vector<double> params = {0, 0, 0, 0}; // initial guess
  double err = 1;
  double temp_err = 0;
  bool cal = false;

  // dispatch procedure
  atomic<int> I_first = 0;
  atomic<int> I_done = 0;
  int s_started = 0;
  int s_done = 0;
  atomic<int> y = 10;

  conn.add_callback([&](const string &f, const string &m)
                    {
    if (m == "CANSTART") {
      conn.send_to(f, "START=" + to_string(s_started++));
    } else if (m.rfind("FINNISH=", 0) == 0) {
      string temp = m.substr(8);
      int i = stoi(temp);
      if (i < I_first) {
        conn.send_to(f, "NO");
      }
      int old, new_;
      do {
        old = I_done;
        if (old >= y || i < I_first) {
          conn.send_to(f, "NO");
        }
        new_ = old + 1;
      } while (I_first.compare_exchange_strong(old, new_));
      if (new_ == y) {
        I_first = s_started;
        I_done = 0;
      }
      s_done++;
      conn.send_to(f, "YES");
    } else if (m.rfind("GRAD=", 0) == 0) {
      vector<double> grad = deserialize(m.substr(5));

      // gradient updation
      for (int i = 0; i < params.size(); i++) {
        params[i] -= lr * grad[i];
      }
      cal = true;
    } else if (m == "GETPARAM") {
      conn.send_to(f, "PARAM=" + serialize(params));
    } else if (m == "GETBATCH") {
      conn.send_to(f, to_string((s_done * 10 + 1) % 499 + 1) + " " +
                          to_string((s_done * 10 + 11) % 499 + 1));
    } else if (m == "End") {
      check--;
    } });

  while (fabs(err - temp_err) > 1e-3)
  {
    if (cal)
    {
      // err calculation
      err = temp_err;
      temp_err = 0;
      vector<vector<double>> all = getbatch(data_file, 1, 500);
      for (vector<double> &v : all)
      {
        double sum = 0;
        for (int i = 0; i < params.size() - 1; i++)
        {
          sum += v[i] * params[i];
        }
        sum += params.back() - v.back();

        temp_err += sum * sum;
      }
      //
      fprintf(log, "[%s] s_done:%d err=%lf\n", node_id.c_str(), s_done, err);
      cal = false;
    }
  }
  conn.broadcast("End");
  fprintf(log, "FINAL PARAMS:");
  for (double &l : params)
  {
    fprintf(log, "%lf ", l);
  }
  fprintf(log, "\n");
  while (check)
  {
  }

  fprintf(log, "[%s] Ended\n", node_id.c_str());
  return 0;
}
