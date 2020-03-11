/**
 * Copyright (c) 2020 by Contributors
 * Authors: access2rohit@gmail.com (Rohit Srivastava)
 *          linhaibin.eric@gmail.com (Haibin Lin)
*/
#ifndef PS_FABRIC_RMA_VAN_H_
#define PS_FABRIC_RMA_VAN_H_

//#ifdef DMLC_USE_RDMA

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <zmq.h>

#include <rdma/rdma_cma.h>

#include <algorithm>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "ps/internal/threadsafe_queue.h"
#include "ps/internal/van.h"


#include <rdma/fi_errno.h>
#include <rdma/fabric.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_tagged.h>

namespace ps {

static const int kStartDepth = 128;
static const int kWriteDepth = kStartDepth;

static const int kRxDepth = kStartDepth * 2;
static const int kReplyDepth = kRxDepth;

static const int kSGEntry = 4;
static const int kTimeoutms = 1000;
static const int kRdmaListenBacklog = 128;
static const int kMaxConcurrentWorkRequest =
    kRxDepth + kStartDepth + kReplyDepth + kWriteDepth;
static const int kMaxHostnameLength = 16;
static const int kMaxDataFields = 4;
static const size_t kAlignment = 8;

static const int kMaxResolveRetry = 50000;
static const int kBasePort = 9010;

// We have a limit of MAX_HANDLE_SIZE = 64 bytes. Therefore, we can only
// support an endpoint name of maximum 56 bytes. We are using remaining
// 8 bytes for tags.
#define FABRIC_MAX_EP_ADDR (56)
#define DMLC_PS_OFI_MAJOR_VERSION  (1)
#define DMLC_PS_OFI_MINOR_VERSION  (6)

#define check_err(ret, msg) do {                          \
        if (ret != 0) {                                   \
          LOG(FATAL) << msg << ". Return Code: " << ret   \
                     << ". ERROR: " << fi_strerror(-ret); \
        }                                                 \
} while (false)

/**
 * \brief be smart on freeing recved data
 */
inline void FreeData2(void* data, void* hint) {
  if (hint == NULL) {
    delete[] static_cast<char*>(data);
  } else {
    delete static_cast<SArray<char>*>(hint);
  }
}

template <typename T>
static inline T align_floor(T v, T align) {
  return v - (v % align);
}

template <typename T>
static inline T align_ceil(T v, T align) {
  return align_floor(v + align - 1, align);
}

enum MessageTypes : uint32_t {
  kRendezvousStart,
  kRendezvousReply,
};

struct RendezvousStart {
  uint64_t meta_len;
  uint64_t data_num;
  uint64_t data_len[kMaxDataFields];
  uint64_t origin_addr;
};

struct RendezvousReply {
  uint64_t addr;
  uint64_t origin_addr;
  uint32_t rkey;
  uint32_t idx;
};

enum WRContextType {
  kRendezvousStartContext,
  kRendezvousReplyContext,
  kWriteContext,
  kReceiveContext
};

struct WRContext {
  WRContextType type;
  struct ibv_mr *buffer;
  void *private_data;
};

struct BufferContext {
  char *buffer;
  size_t meta_len;
  size_t data_num;
  size_t data_len[kMaxDataFields];
};

typedef std::unique_ptr<struct ibv_mr, std::function<void(struct ibv_mr *)>>
    MRPtr;

struct MessageBuffer {
  size_t inline_len;
  char *inline_buf;
  WRContext *reserved_context;
  std::vector<SArray<char>> data;
  std::vector<std::pair<MRPtr, size_t>> mrs;
};

struct RequestContext {
  uint32_t node;
  uint16_t port;
  char hostname[kMaxHostnameLength];
};

static_assert(std::is_pod<RendezvousStart>::value,
              "RendezvousStart must be a POD type.");
static_assert(std::is_pod<RendezvousReply>::value,
              "RendezvousReply must be a POD type.");
static_assert(std::is_pod<RequestContext>::value,
              "RequestContext must be a POD type.");

static const size_t kMempoolChunkSize =
    std::max(sizeof(RendezvousStart), sizeof(RendezvousReply));

template <typename T>
class AddressPool {
 public:
  AddressPool() {
    std::lock_guard<std::mutex> lk(mu_);
    for (int i = 0; i < kMaxEntries; i++) {
      indices_.push(i);
      table_[i] = nullptr;
    }
  }

  T *GetAddressAndRelease(uint32_t index) {
    std::lock_guard<std::mutex> lk(mu_);
    T *ptr = table_[index];
    CHECK(ptr);
    indices_.push(index);
    table_[index] = nullptr;
    return ptr;
  }

  uint32_t StoreAddress(T *ptr) {
    std::lock_guard<std::mutex> lk(mu_);
    CHECK(ptr);
    uint32_t idx = indices_.front();
    indices_.pop();
    CHECK_EQ(table_[idx], nullptr);
    table_[idx] = ptr;
    return idx;
  }

 private:
  static const int kMaxEntries = 512;

  std::mutex mu_;
  std::queue<uint32_t> indices_;
  T *table_[kMaxEntries];
};


#define DIVUP(x, y) (((x)+(y)-1)/(y))
#define ROUNDUP(x, y) (DIVUP((x), (y))*(y))

static inline void fabric_malloc(void** ptr, size_t size) {
  size_t page_size = sysconf(_SC_PAGESIZE);
  void* p;
  int size_aligned = ROUNDUP(size, page_size);
  int ret = posix_memalign(&p, page_size, size_aligned);
  CHECK_EQ(ret, 0) << "posix_memalign error: " << strerror(ret);
  CHECK(p);
  memset(p, 0, size);
  *ptr = p;
}

class FabricMemoryAllocator {
 public:
  explicit FabricMemoryAllocator() {}

  ~FabricMemoryAllocator() {}

  char *Alloc(size_t size) {
    if (size == 0) {
      return nullptr;
    }

    // align to page size (usually 4KB)
    size = align_ceil(size, pagesize_);

    char *p;
    fabric_malloc((void**) &p, size);
    CHECK(p);

    std::lock_guard<std::mutex> lk(mu_);
    used_list.emplace(p, size);
    return p;
  }

  std::mutex mu_;
  size_t pagesize_ = sysconf(_SC_PAGESIZE);
  std::unordered_map<char *, size_t> used_list;
};


struct FabricAddr {
  // endpoint name
  char name[FABRIC_MAX_EP_ADDR] = {};
  // length of endpoint name
  size_t len = sizeof(name);

  std::string DebugStr() const {
    std::string debug_str = "";
    for (size_t i = 0; i < len; i++) {
      debug_str += std::to_string(name[i]) + ",";
    }
    return debug_str;
  }
};

struct FabricContext {
  // fabric provider info
  struct fi_info *info;
  // fabric top-level object
  struct fid_fabric *fabric;
  // domains which maps to a specific local network interface adapter
  struct fid_domain *domain;
  // completion queue
  struct fid_cq *cq;
  // address vector
  struct fid_av *av;
  // the endpoint
  struct fid_ep *ep;
  // endpoint name
  struct FabricAddr addr;
  // readable endpoint name
  struct FabricAddr readable_addr;

  void Init() {
    struct fi_info *hints = nullptr;
    struct fi_cq_attr cq_attr = {};
    struct fi_av_attr av_attr = {};
    int fi_version, ret;

    // set hints for capacity and modes, create fabric, domain and cq
    hints = fi_allocinfo();
    CHECK(hints != nullptr) << "Failed to allocate hints";

    // hints to filter providers
    hints->ep_attr->type = FI_EP_RDM;
    hints->caps = FI_TAGGED | FI_MSG;
    hints->mode = FI_CONTEXT;
    hints->domain_attr->av_type = FI_AV_TABLE;
    hints->domain_attr->control_progress = FI_PROGRESS_AUTO;
    hints->domain_attr->data_progress = FI_PROGRESS_AUTO;
    hints->tx_attr->msg_order = FI_ORDER_SAS;
    hints->rx_attr->msg_order = FI_ORDER_SAS;

    // request for EFA as the provider
    hints->fabric_attr->prov_name = strdup("efa");
    fi_version = FI_VERSION(1, 8);

    // Initialize tag and num_cqes
//    fabric_context_->tag = 1;
//    fabric_context_->num_cqes = DMLC_PS_OFI_MAX_REQUESTS;
//    fabric_context_->prov_name = ofi_provider_->fabric_attr->prov_name;

    // Determine if any tag bits are used by provider
//    int ofi_tag_leading_zeroes = 0, ofi_tag_bits_for_ring_id = 64;
//    while (!((ofi_provider_->ep_attr->mem_tag_format << ofi_tag_leading_zeroes++) &
//      (uint64_t) OFI_HIGHEST_TAG_BIT) &&
//      (ofi_tag_bits_for_ring_id >= MIN_TAG_BITS_FOR_RING_ID)) {
//      ofi_tag_bits_for_ring_id--;
//    }
//
//    CHECK_GT(ofi_tag_bits_for_ring_id, MIN_TAG_BITS_FOR_RING_ID)
//      << "Provider " << ofi_provider_->fabric_attr->prov_name
//      << " does not provide enough tag bits " << ofi_tag_bits_for_ring_id
//      << " for ring ID. Minimum required is " << MIN_TAG_BITS_FOR_RING_ID;
//
//    // Set maximum tag information; Reserving 1 bit for control information
//    fabric_context_->max_tag = (uint64_t)((1ULL << (ofi_tag_bits_for_ring_id - 1)) - 1);

    // fi_getinfo
    ret = fi_getinfo(fi_version, nullptr, 0, 0, hints, &info);
    if (ret == -FI_ENODATA) {
      LOG(FATAL) << "Could not find any optimal provider";
      return;
    }
    check_err(ret, "fi_getinfo failed");
    fi_freeinfo(hints);

    // fi_fabric: create fabric
    ret = fi_fabric(info->fabric_attr, &fabric, nullptr);
    check_err(ret, "Couldn't open a fabric provider");

    // fi_domain: create domain
    ret = fi_domain(fabric, info, &domain, nullptr);
    check_err(ret, "Couldn't open a fabric access domain");

    // fi_av_open: create address vector
    av_attr.type = FI_AV_TABLE;
    ret = fi_av_open(domain, &av_attr, &av, nullptr);
    check_err(ret, "Couldn't open AV");

    // fi_cq_open: open completion queue
    cq_attr.format = FI_CQ_FORMAT_TAGGED;
    ret = fi_cq_open(domain, &cq_attr, &cq, nullptr);
    check_err(ret, "Couldn't open CQ");

    // fi_endpoint: create transport level communication endpoint(s)
    ret = fi_endpoint(domain, info, &ep, nullptr);
    check_err(ret, "Couldn't allocate endpoint");

    // fi_ep_bind: bind CQ and AV to the endpoint
    ret = fi_ep_bind(ep, (fid_t) cq, FI_SEND | FI_RECV);
    check_err(ret, "Couldn't bind EP-CQ");
    ret = fi_ep_bind(ep, (fid_t) av, 0);
    check_err(ret, "Couldn't bind EP-AV");

    // fi_enable: enable endpoint for communication
    ret = fi_enable(ep);
    check_err(ret, "Couldn't enable endpoint");

    // fi_getname: get endpoint name
    ret = fi_getname((fid_t) ep, addr.name, &addr.len);
    check_err(ret, "Call to fi_getname() failed");
    // fi_av_straddr: human readable name
    fi_av_straddr(av, addr.name, readable_addr.name, &readable_addr.len);
    LOG(INFO) << "Endpoint created."
              << "\nendpoint = " << addr.DebugStr()
              << "\nreadable endpoint = "
              << std::string(readable_addr.name, readable_addr.len);
  }

  void Close() {
    // fi_close((fid_t)ep);
    // fi_close((fid_t)cq);
    // fi_close((fid_t)av);
    // fi_close((fid_t)domain);
    // fi_close((fid_t)fabric);
    // fi_freeinfo(fi);
  }
};

class FabricTransport;
struct FabricMemoryAllocator;


struct FabricEndpoint {
  enum ConnectionStatus { IDLE, CONNECTING, CONNECTED, REJECTED };
  ConnectionStatus status;

  int node_id = Node::kEmpty;
  std::string hostport;
  std::condition_variable cv;
  std::mutex connect_mu;
  std::shared_ptr<FabricTransport> trans;
  fi_addr_t peer_addr;

  // WRContext start_ctx[kStartDepth];
  // WRContext reply_ctx[kReplyDepth];

  void Init(const char* address_vector, struct fid_av *av) {
    int ret = fi_av_insert(av, address_vector, 1, //address_vector.c_str(), 1,
                           &peer_addr, 0, nullptr);
    if (ret != 1) {
      LOG(FATAL) << "Call to fi_av_insert() failed. Return Code: "
                 << ret << ". ERROR: " << fi_strerror(-ret);
    }
  }

  void PostRecv(WRContext *ctx) {
    // fi_trecv(struct fid_ep *ep, void *buf, size_t len, void *desc,
    //          fi_addr_t src_addr, uint64_t tag, uint64_t ignore, void *context);
  }

  void SetNodeID(int id) { node_id = id; }

  void SetHostPort(std::string hp) { hostport = hp; }

  void SetTransport(std::shared_ptr<FabricTransport> t) { trans = t; }
};

class FabricTransport {
 public:
  explicit FabricTransport(FabricEndpoint *endpoint, FabricMemoryAllocator *allocator) {
    endpoint_ = CHECK_NOTNULL(endpoint);
    allocator_ = CHECK_NOTNULL(allocator);
    pagesize_ = sysconf(_SC_PAGESIZE);

    auto val = Environment::Get()->find("DMLC_ROLE");
    std::string role(val);
    is_server_ = (role=="server");
  };

  ~FabricTransport() {};

  void Send(struct fid_ep *ep) {
    char* large_buff = allocator_->Alloc(4096);
    int ret = fi_send(ep, large_buff, 4096, nullptr, endpoint_->peer_addr, nullptr);
    if (ret == -FI_EAGAIN) {
      LOG(INFO) << "FI_EAGAIN";
    } else if (ret != 0) {
      check_err(ret, "Unable to do fi_send message");
    } else {
      LOG(INFO) << "Sent one buff";
    }
    return;
  }

 protected:
  size_t pagesize_ = 8192;
  FabricEndpoint *endpoint_;
  FabricMemoryAllocator *allocator_;
  bool is_server_;

}; // class Transport
class FabricRMAVan : public Van {
 public:
  FabricRMAVan() {}
  ~FabricRMAVan() {}

 protected:
  void Start(int customer_id, bool standalone) override {
    start_mu_.lock();
    should_stop_ = false;

    auto val = Environment::Get()->find("DMLC_ROLE");
    std::string role(val);
    LOG(INFO) << "This is a " << role;

    val = Environment::Get()->find("ENABLE_RDMA_LOG");
    enable_rdma_log_ = val? atoi(val) : false;
    if (enable_rdma_log_) LOG(INFO) << "Enable RDMA logging";
    else LOG(INFO) << "RDMA logging is disabled, you can enable it with ENABLE_RDMA_LOG=1";

    start_mu_.unlock();
    zmq_ = Van::Create("zmq");
    zmq_->Start(customer_id, true);
    Van::Start(customer_id, false);
  }

  void Stop() override {
    PS_VLOG(1) << my_node_.ShortDebugString() << " is stopping";
    Van::Stop();

    should_stop_ = true;
    CHECK(should_stop_);

    PS_VLOG(1) << "Stopping cq_polling_thread_.";
    cq_polling_thread_->join();
    cq_polling_thread_.reset();

    PS_VLOG(1) << "Stopping cm_event_polling_thread_.";
    event_polling_thread_->join();
    event_polling_thread_.reset();

    PS_VLOG(1) << "Clearing mempool.";
    mem_allocator_.reset();


//    auto map_iter = memory_mr_map.begin();
//    while (map_iter != memory_mr_map.end()) {
//      ibv_dereg_mr(map_iter->second);
//      map_iter++;
//    }
//
//    PS_VLOG(1) << "Clearing endpoints.";
//    incoming_.clear();
//    endpoints_.clear();
//

//    PS_VLOG(1) << "Destroying cq and pd.";
//    CHECK(!ibv_destroy_cq(cq_)) << "Failed to destroy CQ";

    zmq_->Stop();
//
//    // TODO: ibv_dealloc_pd sometimes complains resource busy, need to fix this
//    // CHECK(!ibv_dealloc_pd(pd_)) << "Failed to deallocate PD: " <<
//    // strerror(errno);
//
//    PS_VLOG(1) << "Destroying listener.";
//    rdma_destroy_id(listener_);
//    rdma_destroy_event_channel(event_channel_);
  }

  int Bind(const Node &node, int max_retry) override {
    std::lock_guard<std::mutex> lock(mu_);
    fabric_context_ = std::unique_ptr<FabricContext>(new FabricContext());
    if (enable_rdma_log_) LOG(INFO) << "Initializing a fabric endpoint";
    CHECK(fabric_context_ != nullptr) << "Failed to allocate Endpoint";

    InitContext();

    int my_port = zmq_->Bind(node, max_retry);
    PS_VLOG(1) << "Done zmq->Bind. My port is " << my_port;
    event_polling_thread_.reset(new std::thread(&FabricRMAVan::PollEvents, this));
    return my_port;
  }

  struct ZmqBufferContext { // for clarity, don't merge meta and data
    std::string sender;
    zmq_msg_t* meta_zmsg;
    std::vector<zmq_msg_t*> data_zmsg;
  };

  void Connect(const Node &node) override {
    CHECK_NE(node.id, node.kEmpty);
    CHECK_NE(node.port, node.kEmpty);
    CHECK(node.hostname.size());
    PS_VLOG(1) << "Connect: " << node.DebugString();
    // worker doesn't need to connect to the other workers. same for server
    if ((node.role == my_node_.role)) {
      return;
    }
    const std::string remote_hostport = host_port(node.hostname, node.port);
    {
      std::lock_guard<std::mutex> lock(mu_);
      hostport_id_map_[remote_hostport] = node.id;
    }

    if (node.id != Node::kEmpty) {
      auto it = endpoints_.find(node.id);

      // if there is an endpoint with pending connection
      if (it != endpoints_.end()) {
        endpoints_.erase(it);
      }

      FabricEndpoint *endpoint;
      endpoints_[node.id] = std::make_unique<FabricEndpoint>();
      endpoint = endpoints_[node.id].get();

      endpoint->SetNodeID(node.id);

      while (endpoint->status != FabricEndpoint::CONNECTED) {
        std::unique_lock<std::mutex> lk(endpoint->connect_mu);
        endpoint->status = FabricEndpoint::CONNECTING;

        // XXX: we re-use the req.meta.control.node to hold connection information:
        Message req;
        req.meta.recver = node.id;
        req.meta.control.cmd = Control::ADDR_REQUEST;
        Node req_info;
        req_info.hostname = my_node_.hostname;
        req_info.port = my_node_.port;
        req_info.aux_id = node.id;
        req.meta.control.node.push_back(req_info);
        // connect zmq. node id is recorded in hostport_id_map_
        zmq_->Connect(node);
        zmq_->Send(req);

        endpoint->cv.wait(lk, [endpoint] {
          return endpoint->status != FabricEndpoint::CONNECTING;
        });

        if (endpoint->status == FabricEndpoint::CONNECTED) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      }
      std::shared_ptr<FabricTransport> t =
          std::make_shared<FabricTransport>(endpoint, mem_allocator_.get());
      endpoint->SetTransport(t);
      t->Send(fabric_context_->ep);
    }
  }

  bool IsValidPushpull(const Message &msg) {
    if (!msg.meta.control.empty()) return false;
    if (msg.meta.simple_app) return false;
    return true;
  }

  uint64_t DecodeKey(SArray<char> keys) { // just a translation, the decoded key might not be readable when we have multiple servers
    ps::Key key = 0;
    uint64_t coef = 1;
    for (unsigned int i = 0; i < keys.size(); ++i) {
      key += coef * (uint8_t) keys.data()[i];
      coef *= 256; // 256=2^8 (uint8_t)
    }
    return key;
  }


  int SendMsg(Message &msg) override {
    return 0;
  }

  int RecvMsg(Message *msg) override {
     return 0;
  }

 private:
  void InitContext() {
    fabric_context_->Init();
    mem_allocator_.reset(new FabricMemoryAllocator());
  }

  //void ReleaseWorkRequestContext(WRContext *context, FabricEndpoint *endpoint) {
  //  switch (context->type) {
  //    case kRendezvousStartContext:
  //      endpoint->free_start_ctx.Push(context);
  //      break;
  //    case kRendezvousReplyContext:
  //      endpoint->free_reply_ctx.Push(context);
  //      break;
  //    case kWriteContext:
  //      endpoint->free_write_ctx.Push(context);
  //      break;
  //    case kReceiveContext:
  //      endpoint->PostRecv(context);
  //      break;
  //    default:
  //      CHECK(0);
  //  }
  //}

  void PollCQ() {
    // Pre-allocated work completions array used for polling
    // TODO: use kMaxConcurrentWorkRequest
    struct fi_cq_err_entry entries[1];
    while (!should_stop_.load()) {
      int ret = fi_cq_read(fabric_context_->cq, entries, 1);
      if (ret == -FI_EAGAIN) {
        continue;
      } else if (ret == -FI_EAVAIL) {
        // TODO: how many errors to read from?
        ret = fi_cq_readerr(fabric_context_->cq, entries, 1);
        if (ret < 0) {
          LOG(FATAL) << "Completion with error";
          //LOG(FATAL) << msg << ". Return Code: " << ret
          //           << ". ERROR: " << fi_strerror(-ret);
        } else {
          LOG(INFO) << "no error?";
        }
      } else {
        if (ret < 0) check_err(ret, "fi_cq_sread failed");
        else {
          LOG(INFO) << ret << " completions";
        }
        //CHK_ERR("fi_cq_read", (ret<0), ret);
        //for (int i = 0; i < ne; ++i) {
        //  CHECK(wc[i].status == IBV_WC_SUCCESS)
        //      << "Failed status \n"
        //      << ibv_wc_status_str(wc[i].status) << " " << wc[i].status << " "
        //      << static_cast<uint64_t>(wc[i].wr_id) << " " << wc[i].vendor_err;

        //  WRContext *context = reinterpret_cast<WRContext *>(wc[i].wr_id);
        //  Endpoint *endpoint =
        //      reinterpret_cast<Endpoint *>(context->private_data);

        //  CHECK(endpoint);

        //  switch (wc[i].opcode) {
        //    default:
        //      CHECK(0) << "Unexpected opcode: " << wc[i].opcode;
        //  }
        //}
      }
    }

  }

  void PollEvents() {
    while (!should_stop_) {
      Message msg;
      int recv_bytes = zmq_->RecvMsg(&msg);
      // For debug, drop received message
      CHECK_NE(recv_bytes, -1) << "unexpected message size " << recv_bytes;
      PS_VLOG(2) << "received ZMQ message " << msg.DebugString();
      CHECK(!msg.meta.control.empty()) << "msg.meta.control is empty";
      auto &ctrl = msg.meta.control;
      if (ctrl.cmd == Control::ADDR_REQUEST) {
        OnConnectRequest(msg);
      } else if (ctrl.cmd == Control::ADDR_RESOLVED) {
        OnConnected(msg);
      } else {
        LOG(FATAL) << "Drop unknown typed message " << msg.DebugString();
      }
    }
  }

  std::string host_port(const std::string& host, const int port) {
    return host + ":" + std::to_string(port);
  }

  std::string get_host(const std::string& host_port) {
    return host_port.substr(0, host_port.find(":"));
  }

  int get_port(const std::string& host_port) {
    std::string port = host_port.substr(host_port.find(":") + 1);
    return std::stoi(port);
  }

  void OnConnected(const Message &msg) {
    const auto& addr_info = msg.meta.control.node[0];
    struct FabricAddr sender_addr;
    sender_addr.len = addr_info.endpoint_name_len;
    memcpy(sender_addr.name, addr_info.endpoint_name, sizeof(sender_addr.name));
    const int sender_id = addr_info.aux_id;

    PS_VLOG(2) << "handling connected reply" << addr_info.DebugString();
    // retrieve endpoint
    FabricEndpoint *endpoint = endpoints_[sender_id].get();
    CHECK(endpoint) << "Endpoint not found.";
    endpoint->Init(sender_addr.name, fabric_context_->av);

    if (cq_polling_thread_ == nullptr) {
      cq_polling_thread_.reset(new std::thread(&FabricRMAVan::PollCQ, this));
    }

    {
      std::lock_guard<std::mutex> lk(endpoint->connect_mu);
      endpoint->status = FabricEndpoint::CONNECTED;
    }
    endpoint->cv.notify_all();
    if (endpoint->node_id != my_node_.id) {
      PS_VLOG(1) << "OnConnected to Node " << endpoint->node_id;
      if (enable_rdma_log_) {
        // fi_av_straddr: human readable name
        // readable address
        struct FabricAddr readable_addr;
        fi_av_straddr(fabric_context_->av, addr_info.endpoint_name,
                      readable_addr.name, &readable_addr.len);
        LOG(INFO) << "Endpoint connected to:" << sender_addr.DebugStr()
                  << "\nreadable addr = "
                  << std::string(readable_addr.name, readable_addr.len);
      }
    }
  }

//  void OnRejected(struct rdma_cm_event *event) {
//    struct rdma_cm_id *id = event->id;
//    Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);
//
//    auto it = endpoints_.find(endpoint->node_id);
//    CHECK(it != endpoints_.end()) << "Connection not ready.";
//    CHECK_EQ(endpoint->status, Endpoint::CONNECTING);
//    CHECK_EQ(endpoint->cm_id, id);
//
//    PS_VLOG(1) << "Connection rejected, retrying...";
//    {
//      std::lock_guard<std::mutex> lk(endpoint->connect_mu);
//      endpoint->status = Endpoint::REJECTED;
//    }
//    endpoint->cv.notify_all();
//  }

  void OnConnectRequest(const Message &msg) {
    const auto& req_info = msg.meta.control.node[0];
    Node addr_info;
    int sender_id;
    // XXX: we reuse req.meta.control.node for connection info
    const std::string req_hostport = host_port(req_info.hostname, req_info.port);
    PS_VLOG(2) << "handling connection request " << req_info.DebugString() << ". " << req_hostport;
    {
      std::lock_guard<std::mutex> lock(mu_);
      // not connected before
      if (hostport_id_map_.find(req_hostport) == hostport_id_map_.end()) {
        sender_id = 30000 + hostport_id_map_.size();
        // connect to the remote node
        Node conn_node;
        // XXX the sender_id is not consistent with the actual node id
        conn_node.id = sender_id;
        conn_node.hostname = req_info.hostname;
        conn_node.port = req_info.port;
        // XXX: make sure the node differs such that connection is not skipped
        if (my_node_.role == Node::SCHEDULER) conn_node.role = Node::WORKER;
        else conn_node.role = Node::SCHEDULER;
        PS_VLOG(1) << "connect to unseen node " << req_hostport << " with id = " << sender_id;
        zmq_->Connect(conn_node);
        hostport_id_map_[req_hostport] = sender_id;
      }
      sender_id = hostport_id_map_[req_hostport];
      addr_info.endpoint_name_len = fabric_context_->addr.len;
      memcpy(addr_info.endpoint_name, fabric_context_->addr.name, sizeof(addr_info.endpoint_name));
      addr_info.aux_id = req_info.aux_id;
    }

    Message reply;
    reply.meta.recver = sender_id;
    reply.meta.control.cmd = Control::ADDR_RESOLVED;
    reply.meta.control.node.push_back(addr_info);
    zmq_->Send(reply);

    const auto r = incoming_.emplace(std::make_unique<FabricEndpoint>());
    FabricEndpoint *endpoint = r.first->get();
    endpoint->SetHostPort(req_hostport);

    char* large_buff[4096];
    int ret = fi_recv(fabric_context_->ep, large_buff, 4096, nullptr, 0, nullptr);
    if (ret == -FI_EAGAIN) {
      LOG(INFO) << "FI_EAGAIN";
    } else if (ret != 0) {
      check_err(ret, "Unable to do fi_recv message");
    }
  }


//  void OnConnectRequest(struct rdma_cm_event *event) {
//    struct rdma_cm_id *id = event->id;
//    CHECK_NOTNULL(id);
//
//    CHECK_LE(sizeof(RequestContext), event->param.conn.private_data_len)
//        << "RequestContext size mismatch. Actual: "
//        << (size_t)event->param.conn.private_data_len
//        << ", Expected: " << sizeof(RequestContext);
//    CHECK_NOTNULL(event->param.conn.private_data);
//
//    const RequestContext *remote_ctx = reinterpret_cast<const RequestContext *>(
//        event->param.conn.private_data);
//
//    const auto r = incoming_.emplace(std::make_unique<Endpoint>());
//    Endpoint *endpoint = r.first->get();
//    endpoint->SetNodeID(remote_ctx->node);
//    endpoint->cm_id = id;
//    id->context = endpoint;
//
//    if (context_ == nullptr) {
//      InitContext(id->verbs);
//    }
//
//    endpoint->Init(cq_, pd_);
//
//    RequestContext ctx;
//    ctx.node = static_cast<uint32_t>(my_node_.id);
//    ctx.port = static_cast<uint16_t>(my_node_.port);
//    snprintf(ctx.hostname, kMaxHostnameLength, "%s", my_node_.hostname.c_str());
//
//    struct rdma_conn_param cm_params;
//    memset(&cm_params, 0, sizeof(cm_params));
//    cm_params.retry_count = 7;
//    cm_params.rnr_retry_count = 7;
//    cm_params.private_data = &ctx;
//    cm_params.private_data_len = sizeof(RequestContext);
//
//    CHECK_EQ(rdma_accept(id, &cm_params), 0)
//        << "Accept RDMA connection failed: " << strerror(errno);
//  }

  // Resolve a route after address is resolved
  void OnAddrResolved(struct rdma_cm_event *event) {
    //struct rdma_cm_id *id = event->id;
    //CHECK_EQ(rdma_resolve_route(id, kTimeoutms), 0)
    //    << "Resolve RDMA route failed";
  }

  // Make a connection after route is resolved
  void OnRouteResolved(struct rdma_cm_event *event) {
    //struct rdma_cm_id *id = event->id;
    //Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);

    //if (context_ == nullptr) {
    //  InitContext(id->verbs);
    //}

    //endpoint->Init(cq_, pd_);

    //RequestContext ctx;
    //ctx.node = static_cast<uint32_t>(my_node_.id);
    //ctx.port = static_cast<uint16_t>(my_node_.port);
    //snprintf(ctx.hostname, kMaxHostnameLength, "%s", my_node_.hostname.c_str());

    //struct rdma_conn_param cm_params;
    //memset(&cm_params, 0, sizeof(cm_params));
    //cm_params.retry_count = 7;
    //cm_params.rnr_retry_count = 7;
    //cm_params.private_data = &ctx;
    //cm_params.private_data_len = sizeof(RequestContext);

    //CHECK_EQ(rdma_connect(id, &cm_params), 0)
    //    << "RDMA connect failed" << strerror(errno);
  }

  void OnDisconnected(struct rdma_cm_event *event) {
  //  LOG(INFO) << "OnDisconnected from Node " << my_node_.id;
  //  struct rdma_cm_id *id = event->id;
  //  Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);
  //  {
  //    std::lock_guard<std::mutex> lk(endpoint->connect_mu);
  //    endpoint->status = Endpoint::IDLE;
  //  }
  //  endpoint->cv.notify_all();
  }


  AddressPool<BufferContext> addr_pool_;
  std::unique_ptr<FabricMemoryAllocator> mem_allocator_;

  std::atomic<bool> should_stop_;

  std::unordered_map<int, std::unique_ptr<FabricEndpoint>> endpoints_;
  std::unordered_set<std::unique_ptr<FabricEndpoint>> incoming_;

  struct rdma_event_channel *event_channel_ = nullptr;
//  struct ibv_context *context_ = nullptr;

  //std::unordered_map<char *, struct ibv_mr *> memory_mr_map;

  // ibverbs protection domain
  struct ibv_pd *pd_ = nullptr;

  // cq thread
  std::unique_ptr<std::thread> cq_polling_thread_;
  // event thread
  std::unique_ptr<std::thread> event_polling_thread_;

  // Recv buffer queue
  // ThreadsafeQueue<std::tuple<Endpoint *, BufferContext *>> recv_buffers_;

  // JYM: the following are for push/pull Fabricbuffer reuse

  // whether my role is server or not
  bool is_server;
  // RDMA logging info
  bool enable_rdma_log_ = false;

  // macros for key_meta_map
  using MetaInfo = std::tuple<int, uint64_t, int>; // len, addr, rkey
  using SenderMeta = std::unordered_map<int, MetaInfo>; // sender as the key
  // (key, sender) --> MetaInfo
  std::unordered_map<ps::Key, SenderMeta> key_meta_map_;
  // a static address for the key
  std::unordered_map<ps::Key, ps::Key> key_addr_map_;
  // a static address for the length
  std::unordered_map<ps::Key, int> key_len_map_;

  std::mutex map_mu_;

  // note that ZMQ use the node id to identify the senders.
  // to setup the connection for libfabric, we don't know the node id ahead of time
  // therefore, we need to use the add sender / receiver hostport to the message meta
  // such that when we unpack the message, we can still know where the message was sent
  // this requires that when calling these APIs:
  // - zmq_->Connect
  // - zmq_->Send
  // - zmq_->RecvMsg
  // we need to make sure req.meta.recver is set correctly.
  // we use hostport_id_map_ to map host:port to IDs. The ID can be arbtrary, as long
  // as the id is unique.
  std::unordered_map<std::string, int> hostport_id_map_;
  Van* zmq_;

  std::unique_ptr<FabricContext> fabric_context_;

  std::mutex mu_;
  bool is_worker_;

  void* receiver_ = nullptr; // for incoming connect queries

  // str = "hostname:port"
  // <key, recver>, (<remote_addr, rkey, idx, local_addr>)
  // std::unordered_map<uint64_t, RemoteAndLocalAddress> push_addr_;
  // std::unordered_map<uint64_t, RemoteAndLocalAddress> pull_addr_;

  std::unordered_map<char*, struct fid_mr*> mem_mr_; // (memory address, fid_mr)
};  // namespace ps
};  // namespace ps

//#endif  // DMLC_USE_RDMA
#endif  // PS_FABRIC_RMA_VAN_H_
