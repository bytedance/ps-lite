// Copyright 2019 Bytedance Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// =============================================================================

#ifndef PS_RDMA_VAN_H_
#define PS_RDMA_VAN_H_

#ifdef DMLC_USE_RDMA

#include "rdma_utils.h"
#include "rdma_transport.h"

namespace ps {

class RDMAVan : public Van {
 public:
  RDMAVan() {
    CHECK_EQ(ibv_fork_init(), 0) << strerror(errno);
  }
  ~RDMAVan() {}

 protected:  
  void Start(int customer_id) override {
    start_mu_.lock();
    should_stop_ = false;

    auto val = Environment::Get()->find("BYTEPS_ENABLE_IPC");
    disable_ipc_ = val ? !atoi(val) : true;
    if (disable_ipc_) LOG(INFO) << "Shared memory IPC has been disabled";

    if (event_channel_ == nullptr) {
      event_channel_ = rdma_create_event_channel();
      CHECK(event_channel_) << "Create RDMA event channel failed";

      cm_event_polling_thread_.reset(
          new std::thread(&RDMAVan::PollEvents, this));
    }

    start_mu_.unlock();
    Van::Start(customer_id);
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
    cm_event_polling_thread_->join();
    cm_event_polling_thread_.reset();

    PS_VLOG(1) << "Clearing mempool.";
    send_mempool_.reset();
    recv_mempool_.reset();

    PS_VLOG(1) << "Clearing endpoints.";
    incoming_.clear();
    endpoints_.clear();

    PS_VLOG(1) << "Destroying cq and pd.";
    CHECK(!ibv_destroy_cq(cq_)) << "Failed to destroy CQ";
    CHECK(!ibv_destroy_comp_channel(comp_event_channel_))
        << "Failed to destroy channel";

    // TODO: ibv_dealloc_pd sometimes complains resource busy, need to fix this
    // CHECK(!ibv_dealloc_pd(pd_)) << "Failed to deallocate PD: " <<
    // strerror(errno);

    PS_VLOG(1) << "Destroying listener.";
    rdma_destroy_id(listener_);
    rdma_destroy_event_channel(event_channel_);
  }

  int Bind(const Node &node, int max_retry) override {
    CHECK(rdma_create_id(event_channel_, &listener_, nullptr, RDMA_PS_TCP) == 0)
        << "Create RDMA connection identifier failed";
    
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));    

    auto val = Environment::Get()->find("DMLC_NODE_HOST");
    if (val) {
      PS_VLOG(1) << "bind to DMLC_NODE_HOST: " << std::string(val);
      addr.sin_addr.s_addr = inet_addr(val);
    } 
    
    addr.sin_family = AF_INET;
    int port = node.port;
    unsigned seed = static_cast<unsigned>(time(NULL) + port);
    for (int i = 0; i < max_retry + 1; ++i) {
      addr.sin_port = htons(port);
      if (rdma_bind_addr(listener_,
                         reinterpret_cast<struct sockaddr *>(&addr)) == 0) {
        break;
      }
      if (i == max_retry) {
        port = -1;
      } else {
        port = 10000 + rand_r(&seed) % 40000;
      }
    }
    CHECK(rdma_listen(listener_, kRdmaListenBacklog) == 0)
        << "Listen RDMA connection failed: " << strerror(errno);
    return port;
  }

  void Connect(const Node &node) override {
    PS_VLOG(1) << "Connecting to Node " << node.id;
    CHECK_NE(node.id, node.kEmpty);
    CHECK_NE(node.port, node.kEmpty);
    CHECK(node.hostname.size());

    // worker doesn't need to connect to the other workers. same for server
    if ((node.role == my_node_.role) && (node.id != my_node_.id)) {
      return;
    }

    if (disable_ipc_) {
      is_local_[node.id] = false;
    } else {
      std::lock_guard<std::mutex> lock(local_mu_);
      is_local_[node.id] = (node.hostname == my_node_.hostname) ? true : false;
      LOG(INFO) << "IPC connected to " << node.id;
    }

    if (node.id != Node::kEmpty) {
      auto it = endpoints_.find(node.id);

      // if there is an endpoint with pending connection
      if (it != endpoints_.end()) {
        endpoints_.erase(it);
      }

      Endpoint *endpoint;
      endpoints_[node.id] = std::make_unique<Endpoint>();
      endpoint = endpoints_[node.id].get();

      endpoint->SetNodeID(node.id);

      struct addrinfo *remote_addr;
      CHECK_EQ(
          getaddrinfo(node.hostname.c_str(), std::to_string(node.port).c_str(),
                      nullptr, &remote_addr),
          0);

      while (endpoint->status != Endpoint::CONNECTED) {
        std::unique_lock<std::mutex> lk(endpoint->connect_mu);
        endpoint->status = Endpoint::CONNECTING;

        if (endpoint->cm_id != nullptr) {
          rdma_destroy_qp(endpoint->cm_id);
          CHECK_EQ(rdma_destroy_id(endpoint->cm_id), 0) << strerror(errno);
          endpoint->cm_id = nullptr;
        }

        CHECK_EQ(rdma_create_id(event_channel_, &endpoint->cm_id, nullptr,
                                RDMA_PS_TCP),
                 0)
            << "Create RDMA connection identifier failed";
        endpoint->cm_id->context = endpoint;

        int max_retry = kMaxResolveRetry;
        int port = kBasePort;
        unsigned seed = static_cast<unsigned>(time(NULL) + port);
        auto val = Environment::Get()->find("DMLC_NODE_HOST");
        if (val) {
          struct sockaddr_in addr;
          memset(&addr, 0, sizeof(addr)); 
          addr.sin_addr.s_addr = inet_addr(val);
          addr.sin_family = AF_INET;
          for (int i = 0; i < max_retry + 1; ++i) {
            addr.sin_port = htons(port);
            if (rdma_resolve_addr(endpoint->cm_id, 
                                  reinterpret_cast<struct sockaddr *>(&addr),
                                  remote_addr->ai_addr, kTimeoutms) == 0) {
              break;
            }
            if (i == max_retry) {
              port = -1;
            } else {
              port = 10000 + rand_r(&seed) % 40000;
            }
          }
        } else {
          CHECK_EQ(rdma_resolve_addr(endpoint->cm_id, nullptr,
                                     remote_addr->ai_addr, kTimeoutms),
                   0)
              << "Resolve RDMA address failed with errno: " << strerror(errno);
        }

        endpoint->cv.wait(lk, [endpoint] {
          return endpoint->status != Endpoint::CONNECTING;
        });

        if (endpoint->status == Endpoint::CONNECTED) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      }

      std::shared_ptr<Transport> t = is_local_[node.id] ?
          std::make_shared<IPCTransport>(endpoint, send_mempool_.get()) :
          std::make_shared<RDMATransport>(endpoint, send_mempool_.get());
      endpoint->SetTransport(t);

      freeaddrinfo(remote_addr);
    }
  }

  void PackWorkerTensorAddress(Message &msg) {
    // must be pull response
    if (msg.meta.push || msg.meta.request) return;
    
    uint64_t key = msg.meta.key;
    auto recver = msg.meta.recver;

    std::lock_guard<std::mutex> lock(info_mu_);
    CHECK_NE(tensor_info_map_.find(key), tensor_info_map_.end())
        << "key=" << key << " not inited in tensor_info_map_";
    CHECK_NE(tensor_info_map_[key].find(recver), tensor_info_map_[key].end())
        << "key=" << key << ", recver=" << recver << " not inited in tensor_info_map_[key]";
    msg.meta.val_len = std::get<0>(tensor_info_map_[key][recver]);
    msg.meta.addr = std::get<1>(tensor_info_map_[key][recver]);
    msg.meta.option = std::get<2>(tensor_info_map_[key][recver]);
  }

  void StoreWorkerTensorAddress(Message *msg) {
    auto key = msg->meta.key;
    auto len = msg->meta.val_len;
    auto addr = msg->meta.addr;
    auto rkey = msg->meta.option;
    auto sender = msg->meta.sender;

    std::lock_guard<std::mutex> lock(info_mu_);
    if (tensor_info_map_.find(key) == tensor_info_map_.end()
          || tensor_info_map_[key].find(sender) == tensor_info_map_[key].end()) {
      tensor_info_map_[key][sender] = std::make_tuple(len, addr, rkey);
    } else {
      CHECK_EQ(len, std::get<0>(tensor_info_map_[key][sender]));
      CHECK_EQ(addr, std::get<1>(tensor_info_map_[key][sender]));
      CHECK_EQ(rkey, std::get<2>(tensor_info_map_[key][sender]));
    }
  }

  bool HasRemoteInfo(MessageBuffer *msg_buf, uint64_t key, bool is_push, int recver) {
    std::lock_guard<std::mutex> lk(addr_mu_);
    if (is_push && (push_addr_.find(key) != push_addr_.end()) 
        && (push_addr_[key].find(recver) != push_addr_[key].end())) {
      return true;
    }
    if (!is_push && (pull_addr_.find(key) != pull_addr_.end()) 
        && (pull_addr_[key].find(recver) != pull_addr_[key].end())) {
      return true;
    }
    // no remote info, store the msg_buf address and push/pull flag for RendezvousReply
    msgbuf_cache_.emplace(reinterpret_cast<uint64_t>(msg_buf), std::make_tuple(key, is_push, recver));
    return false;
  }

  void StoreRemoteInfo(MessageBuffer *msg_buf, uint64_t remote_addr, uint32_t rkey, uint32_t idx) {
    auto buf = reinterpret_cast<uint64_t>(msg_buf);
    if (msgbuf_cache_.find(buf) == msgbuf_cache_.end()) return; // control message
    std::lock_guard<std::mutex> lk(addr_mu_);
    auto key = std::get<0>(msgbuf_cache_[buf]);
    auto is_push = std::get<1>(msgbuf_cache_[buf]);
    auto recver = std::get<2>(msgbuf_cache_[buf]);
    if (is_push) {
      push_addr_[key][recver] = std::make_tuple(remote_addr, rkey, idx);
    } else {
      pull_addr_[key][recver] = std::make_tuple(remote_addr, rkey, idx);
    }
    CHECK_NE(msgbuf_cache_.find(buf), msgbuf_cache_.end());
    msgbuf_cache_.erase(buf);
  }

  RemoteAddress GetRemoteInfo(uint64_t key, bool is_push) {
    std::lock_guard<std::mutex> lk(addr_mu_);
    return (is_push ? push_addr_[key] : pull_addr_[key]);
  }

  int SendMsg(Message &msg) override {
    int remote_id = msg.meta.recver;
    CHECK_NE(remote_id, Meta::kEmpty);
    CHECK_NE(endpoints_.find(remote_id), endpoints_.end());
    Endpoint *endpoint = endpoints_[remote_id].get();

    auto trans = CHECK_NOTNULL(endpoint->GetTransport());
    trans->RegisterMemory(msg);

    MessageBuffer *msg_buf = new MessageBuffer();

    int meta_len = GetPackMetaLen(msg.meta);

    size_t data_len = msg.meta.data_size;
    size_t total_len = meta_len + data_len;
    CHECK(meta_len);

    msg_buf->inline_len = meta_len;
    msg_buf->inline_buf = send_mempool_->Alloc(meta_len);
    msg_buf->data = msg.data;

    if (IsValidPushpull(msg)) {
      trans->AddMeta(msg);
      PackWorkerTensorAddress(msg);
    }

    PackMeta(msg.meta, &(msg_buf->inline_buf), &meta_len);

    if (!IsValidPushpull(msg)) { 
      trans->SendRendezvousBegin(msg, msg_buf);
      return total_len;
    } else {
      trans->PrepareData(msg, msg_buf);
      auto is_push = msg.meta.push;
      auto key = msg.meta.key;
      if (!HasRemoteInfo(msg_buf, key, is_push, remote_id)) {
        trans->SendRendezvousBegin(msg, msg_buf);
        return total_len;
      }
    }

    auto remote_addr_tuple = GetRemoteInfo(msg.meta.key, msg.meta.push);

    // already know remote address, directly use RDMA-write 
    if (msg.meta.push && msg.meta.request) { 
      // worker, push request
      trans->SendPushRequest(msg, msg_buf, remote_addr_tuple);
    } else if (msg.meta.push && !msg.meta.request) { 
      // server, push response
      trans->SendPushResponse(msg, msg_buf, remote_addr_tuple);
    } else if (!msg.meta.push && msg.meta.request) { 
      // worker, pull request
      trans->SendPullRequest(msg, msg_buf, remote_addr_tuple);
    } else if (!msg.meta.push && !msg.meta.request) { 
      // server, pull response
      trans->SendPullResponse(msg, msg_buf, remote_addr_tuple);
    } else {
      CHECK(0) << "unexpected message type";
    }

    return total_len;
  }

  int RecvMsg(Message *msg) override {
    msg->data.clear();
    std::tuple<Endpoint *, BufferContext *> notification;
    recv_buffers_.WaitAndPop(&notification);

    Endpoint *endpoint = std::get<Endpoint *>(notification);
    BufferContext *buffer_ctx = std::get<BufferContext *>(notification);

    msg->meta.recver = my_node_.id;
    msg->meta.sender = endpoint->node_id;

    // the second argument is actually deprecated, 
    // we keep it as is in order to be compatible    
    UnpackMeta(buffer_ctx->buffer, buffer_ctx->meta_len, &msg->meta); 
    int meta_len = GetPackMetaLen(msg->meta);

    int total_len = 0;
    total_len += meta_len;

    auto trans = CHECK_NOTNULL(endpoint->GetTransport());

    if (!IsValidPushpull(*msg)) {
      recv_mempool_->Free(buffer_ctx->buffer);
      delete buffer_ctx;
      return total_len;
    }

    // valid data message
    if (msg->meta.push && msg->meta.request) { 
      // push request
      total_len += trans->RecvPushRequest(msg, buffer_ctx, meta_len);
      StoreWorkerTensorAddress(msg);
    } else if (!msg->meta.push && msg->meta.request) { 
      // pull request
      total_len += trans->RecvPullRequest(msg, buffer_ctx, meta_len);
    } else if (msg->meta.push && !msg->meta.request) { 
      // push response
      total_len += trans->RecvPushResponse(msg, buffer_ctx, meta_len);
    } else if (!msg->meta.push && !msg->meta.request) { 
      // pull response
      total_len += trans->RecvPullResponse(msg, buffer_ctx, meta_len);
    } else {
      CHECK(0) << "unknown msg type";
    }

    return total_len;
  }

 private:
  void InitContext(struct ibv_context *context) {
    context_ = context;
    CHECK(context_) << "ibv_context* empty";

    pd_ = ibv_alloc_pd(context_);
    CHECK(pd_) << "Failed to allocate protection domain";

    send_mempool_.reset(new SimpleMempool(pd_));
    recv_mempool_.reset(new SimpleMempool(pd_));

    comp_event_channel_ = ibv_create_comp_channel(context_);

    // TODO(clan): Replace the rough estimate here
    cq_ = ibv_create_cq(context_, kMaxConcurrentWorkRequest * 2, NULL,
                        comp_event_channel_, 0);

    CHECK(cq_) << "Failed to create completion queue";
    CHECK(!ibv_req_notify_cq(cq_, 0)) << "Failed to request CQ notification";
  }

  void ReleaseWorkRequestContext(WRContext *context, Endpoint *endpoint) {
    switch (context->type) {
      case kRendezvousStartContext:
        endpoint->free_start_ctx.Push(context);
        break;
      case kRendezvousReplyContext:
        endpoint->free_reply_ctx.Push(context);
        break;
      case kWriteContext:
        endpoint->free_write_ctx.Push(context);
        break;
      case kReceiveContext:
        endpoint->PostRecv(context);
        break;
      default:
        CHECK(0);
    }
  }

  void PollCQ() {
    // Pre-allocated work completions array used for polling
    struct ibv_wc wc[kMaxConcurrentWorkRequest];
    while (!should_stop_.load()) {
      int ne = ibv_poll_cq(cq_, kMaxConcurrentWorkRequest, wc);
      CHECK_GE(ne, 0);
      for (int i = 0; i < ne; ++i) {
        CHECK(wc[i].status == IBV_WC_SUCCESS)
            << "Failed status \n"
            << ibv_wc_status_str(wc[i].status) << " " << wc[i].status << " "
            << static_cast<uint64_t>(wc[i].wr_id) << " " << wc[i].vendor_err;

        WRContext *context = reinterpret_cast<WRContext *>(wc[i].wr_id);
        Endpoint *endpoint =
            reinterpret_cast<Endpoint *>(context->private_data);

        CHECK(endpoint);

        switch (wc[i].opcode) {
          case IBV_WC_SEND:
            ReleaseWorkRequestContext(context, endpoint);
            break;
          case IBV_WC_RDMA_WRITE: {
            MessageBuffer *msg_buf =
                *reinterpret_cast<MessageBuffer **>(context->buffer->addr);
            send_mempool_->Free(msg_buf->inline_buf);
            delete msg_buf;
            ReleaseWorkRequestContext(context, endpoint);
          } break;
          case IBV_WC_RECV_RDMA_WITH_IMM: {
            uint32_t addr_idx = wc[i].imm_data;
            BufferContext *buf_ctx = addr_pool_.GetAddress(addr_idx);
            recv_buffers_.Push(std::make_tuple(endpoint, buf_ctx));
            ReleaseWorkRequestContext(context, endpoint);
          } break;
          case IBV_WC_RECV: {
            CHECK(wc[i].wc_flags & IBV_WC_WITH_IMM);
            uint32_t imm = wc[i].imm_data;
            struct ibv_mr *mr = context->buffer;

            if (imm == kRendezvousStart) {
              RendezvousStart *req =
                  reinterpret_cast<RendezvousStart *>(mr->addr);
              auto trans = CHECK_NOTNULL(endpoint->GetTransport());
              trans->SendRendezvousReply(req, addr_pool_);
              
            } else if (imm == kRendezvousReply) {
              auto trans = CHECK_NOTNULL(endpoint->GetTransport());
              RendezvousReply *resp =
                  reinterpret_cast<RendezvousReply *>(mr->addr);
              uint64_t remote_addr = resp->addr;
              uint64_t origin_addr = resp->origin_addr;
              uint32_t rkey = resp->rkey;
              uint32_t idx = resp->idx;

              MessageBuffer *msg_buf =
                  reinterpret_cast<MessageBuffer *>(origin_addr);

              // Before RDMA write, store the remote info so that 
              // subsequent write does not need repeated rendezvous 
              StoreRemoteInfo(msg_buf, remote_addr, rkey, idx);
              trans->RDMAWriteWithImm(msg_buf, remote_addr, rkey, idx);
            } else {
              CHECK(0);
            }
            ReleaseWorkRequestContext(context, endpoint);
          } break;
          default:
            CHECK(0) << "Unexpected opcode: " << wc[i].opcode;
        }
      }
    }
  }

  void PollEvents() {
    int flags = fcntl(event_channel_->fd, F_GETFL);
    int rc = fcntl(event_channel_->fd, F_SETFL, flags | O_NONBLOCK);
    CHECK_GE(rc, 0);
    int error_flags = POLLERR | POLLHUP | POLLNVAL;

    while (!should_stop_.load()) {
      struct pollfd pfd = {
          .fd = event_channel_->fd, .events = POLLIN, .revents = 0};
      int ret = poll(&pfd, 1, 10);

      CHECK_GE(ret, 0) << strerror(errno);
      CHECK_EQ(pfd.revents & error_flags, 0);

      if (!(pfd.revents & POLLIN)) {
        continue;
      }

      struct rdma_cm_event *event;
      CHECK_EQ(rdma_get_cm_event(event_channel_, &event), 0);
      // TODO(clan): Reorder the list according to the event frequency
      switch (event->event) {
        case RDMA_CM_EVENT_CONNECT_REQUEST:
          OnConnectRequest(event);
          break;
        case RDMA_CM_EVENT_ADDR_RESOLVED:
          OnAddrResolved(event);
          break;
        case RDMA_CM_EVENT_ROUTE_RESOLVED:
          OnRouteResolved(event);
          break;
        case RDMA_CM_EVENT_ESTABLISHED:
          OnConnected(event);
          break;
        case RDMA_CM_EVENT_DISCONNECTED:
          OnDisconnected(event);
          break;
        case RDMA_CM_EVENT_REJECTED:
          OnRejected(event);
          break;
        default:
          CHECK(0) << "OnEvent: unknown event " << event->event << " ("
                   << rdma_event_str(event->event) << ")";
      }
      rdma_ack_cm_event(event);
    }
  }

  void OnRejected(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);

    auto it = endpoints_.find(endpoint->node_id);
    CHECK(it != endpoints_.end()) << "Connection not ready.";
    CHECK_EQ(endpoint->status, Endpoint::CONNECTING);
    CHECK_EQ(endpoint->cm_id, id);

    PS_VLOG(1) << "Connection rejected, retrying...";
    {
      std::lock_guard<std::mutex> lk(endpoint->connect_mu);
      endpoint->status = Endpoint::REJECTED;
    }
    endpoint->cv.notify_all();
  }

  void OnConnectRequest(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    CHECK_NOTNULL(id);

    CHECK_LE(sizeof(RequestContext), event->param.conn.private_data_len)
        << "RequestContext size mismatch. Actual: "
        << (size_t)event->param.conn.private_data_len
        << ", Expected: " << sizeof(RequestContext);
    CHECK_NOTNULL(event->param.conn.private_data);

    const RequestContext *remote_ctx = reinterpret_cast<const RequestContext *>(
        event->param.conn.private_data);

    const auto r = incoming_.emplace(std::make_unique<Endpoint>());
    Endpoint *endpoint = r.first->get();
    endpoint->SetNodeID(remote_ctx->node);
    endpoint->cm_id = id;
    id->context = endpoint;

    if (context_ == nullptr) {
      InitContext(id->verbs);
    }

    endpoint->Init(cq_, pd_);

    std::shared_ptr<Transport> t = is_local_[remote_ctx->node] ?
        std::make_shared<IPCTransport>(endpoint, recv_mempool_.get()) :
        std::make_shared<RDMATransport>(endpoint, recv_mempool_.get());
    endpoint->SetTransport(t);

    RequestContext ctx;
    ctx.node = static_cast<uint32_t>(my_node_.id);
    ctx.port = static_cast<uint16_t>(my_node_.port);
    snprintf(ctx.hostname, kMaxHostnameLength, "%s", my_node_.hostname.c_str());

    struct rdma_conn_param cm_params;
    memset(&cm_params, 0, sizeof(cm_params));
    cm_params.retry_count = 7;
    cm_params.rnr_retry_count = 7;
    cm_params.private_data = &ctx;
    cm_params.private_data_len = sizeof(RequestContext);

    CHECK_EQ(rdma_accept(id, &cm_params), 0)
        << "Accept RDMA connection failed: " << strerror(errno);
  }

  // Resolve a route after address is resolved
  void OnAddrResolved(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    CHECK_EQ(rdma_resolve_route(id, kTimeoutms), 0)
        << "Resolve RDMA route failed";
  }

  // Make a connection after route is resolved
  void OnRouteResolved(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);
    
    if (context_ == nullptr) {
      InitContext(id->verbs);
    }

    endpoint->Init(cq_, pd_);

    RequestContext ctx;
    ctx.node = static_cast<uint32_t>(my_node_.id);
    ctx.port = static_cast<uint16_t>(my_node_.port);
    snprintf(ctx.hostname, kMaxHostnameLength, "%s", my_node_.hostname.c_str());

    struct rdma_conn_param cm_params;
    memset(&cm_params, 0, sizeof(cm_params));
    cm_params.retry_count = 7;
    cm_params.rnr_retry_count = 7;
    cm_params.private_data = &ctx;
    cm_params.private_data_len = sizeof(RequestContext);

    CHECK_EQ(rdma_connect(id, &cm_params), 0)
        << "RDMA connect failed" << strerror(errno);
  }

  void OnConnected(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    CHECK(id) << "rdma_cm_id not found.";
    Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);
    CHECK(endpoint) << "Endpoint not found.";

    if (cq_polling_thread_ == nullptr) {
      cq_polling_thread_.reset(new std::thread(&RDMAVan::PollCQ, this));
    }

    CHECK_EQ(endpoint->cm_id, id);
    {
      std::lock_guard<std::mutex> lk(endpoint->connect_mu);
      endpoint->status = Endpoint::CONNECTED;
    }
    endpoint->cv.notify_all();
    if (endpoint->node_id != my_node_.id) {
      PS_VLOG(1) << "OnConnected to Node " << endpoint->node_id;
    }
  }

  void OnDisconnected(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);
    {
      std::lock_guard<std::mutex> lk(endpoint->connect_mu);
      endpoint->status = Endpoint::IDLE;
    }
    endpoint->cv.notify_all();
    LOG(INFO) << "OnDisconnected from Node " << endpoint->node_id;
  }

  AddressPool<BufferContext> addr_pool_;
  std::unique_ptr<SimpleMempool> recv_mempool_;
  std::unique_ptr<SimpleMempool> send_mempool_;

  std::unique_ptr<RDMATransport> rdma_trans_;
  std::unique_ptr<IPCTransport> ipc_trans_;

  struct rdma_cm_id *listener_ = nullptr;
  std::atomic<bool> should_stop_;

  std::unordered_map<int, std::unique_ptr<Endpoint>> endpoints_;
  std::unordered_set<std::unique_ptr<Endpoint>> incoming_;

  struct rdma_event_channel *event_channel_ = nullptr;
  struct ibv_context *context_ = nullptr;

  // ibverbs protection domain
  struct ibv_pd *pd_ = nullptr;
  // Completion event channel, to wait for work completions
  struct ibv_comp_channel *comp_event_channel_ = nullptr;
  // Completion queue, to poll on work completions
  struct ibv_cq *cq_ = nullptr;
  // cq thread
  std::unique_ptr<std::thread> cq_polling_thread_;
  // event thread
  std::unique_ptr<std::thread> cm_event_polling_thread_;
  // Recv buffer queue
  ThreadsafeQueue<std::tuple<Endpoint *, BufferContext *>> recv_buffers_;

  // local IPC related
  bool disable_ipc_ = false;
  std::mutex local_mu_;
  std::unordered_map<int, bool> is_local_;

  // worker's tensor address
  std::mutex info_mu_;
  using TensorInfo = std::tuple<int, uint64_t, int>; // len, addr, rkey
  using RemoteTensorMeta = std::unordered_map<int, TensorInfo>; // sender as the key
  std::unordered_map<ps::Key, RemoteTensorMeta> tensor_info_map_; // (key, sender) --> TensorInfo

  // store rendezvous address
  std::mutex addr_mu_;
  std::unordered_map<uint64_t, RemoteAddress> push_addr_; // <key, recver>, <remote_addr, rkey, idx>
  std::unordered_map<uint64_t, RemoteAddress> pull_addr_; // <key, recver>, <remote_addr, rkey, idx>
  std::unordered_map<uint64_t, std::tuple<uint64_t, bool, int> > msgbuf_cache_; // msg_buf, <key, is_push, recver>
};  // class RDMAVan

};  // namespace ps

#endif  // DMLC_USE_RDMA
#endif  // PS_RDMA_VAN_H_
