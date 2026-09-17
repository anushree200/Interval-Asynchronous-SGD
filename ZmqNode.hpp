#ifndef ZMQNODE_HPP
#define ZMQNODE_HPP

#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <zmq.hpp>

class ZmqNode {
private:
  std::string node_id;
  zmq::context_t ctx;
  zmq::socket_t router;

  std::string ip;
  std::string port;

  std::unordered_map<std::string, zmq::socket_t> peer_dealer;
  std::unordered_map<std::string, std::string> peers;
  std::mutex peer_mutex;

  std::vector<std::function<void(const std::string &, const std::string &)>>
      callbacks;
  std::mutex callback_mutex;

  std::thread recv_thread;
  std::atomic<bool> running;

  static std::unordered_map<std::string, std::string> global_peer_map;
  static std::mutex global_peer_mutex;

  bool verbose;

public:
  ZmqNode(const std::string &id, const std::string &bind_ip,
          const std::string &bind_port, bool verbose = false)
      : node_id(id), ctx(1), router(ctx, zmq::socket_type::router),
        peer_dealer(), running(true), verbose(verbose),
        ip((bind_ip != "*") ? bind_ip : "localhost"), port(bind_port) {
    router.set(zmq::sockopt::routing_id, node_id);
    router.set(zmq::sockopt::linger, 0);
    router.bind("tcp://" + bind_ip + ":" + port);

    {
      std::lock_guard<std::mutex> lock(global_peer_mutex);
      global_peer_map[node_id] = "tcp://" + ip + ":" + port;
    }

    recv_thread = std::thread(&ZmqNode::receiver_loop, this);

    printf("[%s] Bound to %s\n", node_id.c_str(), (ip + ":" + port).c_str());
  }

  ~ZmqNode() {
    running = false;
    if (recv_thread.joinable()) {
      recv_thread.join();
    }

    for (auto &[peer_id, dealer] : peer_dealer) {
      dealer.close();
    }
    router.close();
    ctx.close();

    printf("[%s] Closed\n", node_id.c_str());
  }

  void connect_to(const std::string &addr, const std::string &peer_id) {
    zmq::socket_t dealer(ctx, zmq::socket_type::dealer);
    dealer.set(zmq::sockopt::routing_id, node_id);
    dealer.set(zmq::sockopt::linger, 0);
    dealer.connect("tcp://" + addr);
    {
      std::lock_guard<std::mutex> lock(peer_mutex);
      peer_dealer[peer_id] = std::move(dealer);
      peers[peer_id] = addr;
    }
    printf("[%s] Connected to %s\n", node_id.c_str(), addr.c_str());
  }

  void send_to(const std::string &peer_id, const std::string &msg) {
    zmq::socket_t *p_dealer;
    if (peer_mutex.try_lock()) {
      lazy_connect(peer_id);
      p_dealer = &peer_dealer[peer_id];
      peer_mutex.unlock();
    } else {
      lazy_connect(peer_id);
      p_dealer = &peer_dealer[peer_id];
    }

    zmq::message_t body(msg.data(), msg.size());

    p_dealer->send(body, zmq::send_flags::none);

    if (verbose)
      printf("[%s] Send to %s : %s\n", node_id.c_str(), peer_id.c_str(),
             msg.c_str());
  }

  void broadcast(const std::string &msg) {
    if (verbose)
      printf("[%s] Broadcasting: %s\n", node_id.c_str(), msg.c_str());
    std::lock_guard<std::mutex> lock(peer_mutex);
    for (const auto &[peer_id, addr] : peers) {
      send_to(peer_id, msg);
    }
  }

  void add_callback(
      const std::function<void(const std::string &, const std::string &)> &cb) {
    std::lock_guard<std::mutex> lock(callback_mutex);
    callbacks.push_back(cb);
  }

  std::unordered_map<std::string, std::string> get_peers() { return peers; }

  static void addrflush() {
    std::lock_guard<std::mutex> lock(global_peer_mutex);
    global_peer_map.clear();
  }

private:
  void lazy_connect(const std::string &peer_id) {
    if (peers.count(peer_id) && peers[peer_id] != "")
      return;

    std::string addr;
    {
      std::lock_guard<std::mutex> g_lock(global_peer_mutex);
      auto it = global_peer_map.find(peer_id);
      if (it != global_peer_map.end()) {
        addr = it->second;
      } else {
        throw std::runtime_error("Unknown peer_id: " + peer_id);
      }
    }

    zmq::socket_t dealer(ctx, zmq::socket_type::dealer);
    dealer.set(zmq::sockopt::routing_id, node_id);
    dealer.set(zmq::sockopt::linger, 0);
    dealer.connect(addr);
    peer_dealer[peer_id] = std::move(dealer);
    peers[peer_id] = addr;

    printf("[%s] Lazy connected to %s\n", node_id.c_str(), addr.c_str());
  }

  void receiver_loop() {
    zmq::pollitem_t items[] = {{router, 0, ZMQ_POLLIN, 0}};
    while (running) {
      zmq::poll(items, 1, std::chrono::milliseconds(100));

      if (items[0].revents & ZMQ_POLLIN) {
        while (router.get(zmq::sockopt::events) & ZMQ_POLLIN) {
          zmq::message_t sender, body;

          (void)router.recv(sender, zmq::recv_flags::none);
          (void)router.recv(body, zmq::recv_flags::none);

          std::string from(static_cast<char *>(sender.data()), sender.size());
          std::string msg(static_cast<char *>(body.data()), body.size());

          {
            std::lock_guard<std::mutex> lock(peer_mutex);
            if (peers.count(from) == 0)
              peers[from] = "";
          }

          std::lock_guard<std::mutex> lock(callback_mutex);
          for (auto &cb : callbacks)
            cb(from, msg);
        }
      }
    }
  }
};

inline std::unordered_map<std::string, std::string> ZmqNode::global_peer_map;
inline std::mutex ZmqNode::global_peer_mutex;

#endif
