#include "ZmqNode.hpp"
#include "helper.hpp"
#include <cstdio>
#include <cstdlib>
#include <sstream>
#include <string>

int main(int argc, char **argv)
{
  using namespace std;
  string node_id;
  string log_file;
  string port;
  string ip = "*";
  string disp_id = "d";
  string disp_ip = "localhost";
  string disp_port;
  string data_file;

  for (int i = 1; i < argc; i++)
  {
    string arg = argv[i];
    if (arg.rfind("-id=", 0) == 0)
    {
      node_id = arg.substr(4);
    }
    else if (arg.rfind("--log=", 0) == 0)
    {
      log_file = arg.substr(6);
    }
    else if (arg.rfind("-port=", 0) == 0)
    {
      port = arg.substr(6);
    }
    else if (arg.rfind("--ip=", 0) == 0)
    {
      ip = arg.substr(5);
    }
    else if (arg.rfind("--disp-id=", 0) == 0)
    {
      disp_id = arg.substr(9);
    }
    else if (arg.rfind("-data=", 0) == 0)
    {
      data_file = arg.substr(6);
    }
    else if (arg.rfind("--disp-ip=", 0) == 0)
    {
      disp_ip = arg.substr(9);
    }
    else if (arg.rfind("-disp-port=", 0) == 0)
    {
      disp_port = arg.substr(11);
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

  if (node_id.empty() || port.empty() || data_file.empty() ||
      disp_port.empty())
  {
    throw runtime_error("No id or port or data_file mentioned, ./<executable> "
                        "-id=name\n"
                        "--log=<log_file: def = <id>.log>\n"
                        "--disp_id=<dispatch name: def=d>\n"
                        "-port=<port_num>\n"
                        "--ip=<ip address: def=localhost>\n"
                        "--disp-ip=<dispatch ip: def = localhost>\n"
                        "-disp-port=<dispatch port>\n"
                        "-data=<data_file>\n");
  }

  FILE *log = fopen(log_file.c_str(), "w");

  ZmqNode conn(node_id, ip, port);
  conn.connect_to(disp_ip + ":" + disp_port, disp_id);
  // worker procedure
  bool isFinished = false;
  bool canstart = false;
  bool canupdate = false;
  bool wait = true;
  int i = -1;
  string serialized_params;
  int batch_st = -1;
  int batch_ed = -1;
  vector<double> grad = {0, 0, 0, 0};

  conn.add_callback([&](const string &f, const string &m)
                    {
    if (m.rfind("START=", 0) == 0) {
      string temp = m.substr(6);
      i = stoi(temp);
      canstart = true;
    } else if (m == "NO") {
      wait = false;
    } else if (m == "YES") {
      wait = false;
      canupdate = true;
    } else if (m == "End") {
      isFinished = true;
    } else if (m.rfind("PARAM=", 0) == 0) {
      serialized_params = m.substr(6);
    } else {
      stringstream ss(m);
      ss >> batch_st;
      ss >> batch_ed;
    } });

  while (!isFinished)
  {
    conn.send_to(disp_id, "CANSTART");
    while (!canstart)
    {
    }
    conn.send_to(disp_id, "GETPARAM");
    while (serialized_params.empty())
    {
    }
    vector<double> params_local = deserialize(serialized_params);
    conn.send_to(disp_id, "GETBATCH");
    while ((batch_st == -1 || batch_ed == -1))
    {
    }
    vector<vector<double>> batch = getbatch(data_file, batch_st, batch_ed);

    grad = {0, 0, 0, 0};
    for (vector<double> &v : batch)
    {
      double sum = 0;
      for (int i = 0; i < params_local.size() - 1; i++)
      {
        sum += params_local[i] * v[i];
      }
      sum += params_local.back() - v.back();
      for (int i = 0; i < params_local.size() - 1; i++)
      {
        grad[i] += sum * v[i];
      }
      grad.back() = sum;
    }
    conn.send_to(disp_id, "FINNISH=" + to_string(i));
    while (wait)
    {
    }
    if (canupdate)
    {
      conn.send_to(disp_id, "GRAD=" + serialize(grad));
    }
    serialized_params.clear();
    wait = true;
    canstart = false;
    canupdate = false;
    batch_ed = batch_st = -1;
  }

  conn.send_to(disp_id, "End");

  fprintf(log, "[%s] Ended\n", node_id.c_str());

  return 0;
}
