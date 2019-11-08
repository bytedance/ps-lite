// Copyright 2019 Bytedance Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2015 by ps-lite Contributors
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

#include "ps/internal/customer.h"
#include "ps/internal/postoffice.h"
#include "ps/internal/threadsafe_queue.h"
#include <map>
#include <atomic>
#include <set>
#include <list>
#include <fstream>
#include <chrono>

namespace ps {
const int Node::kEmpty = std::numeric_limits<int>::max();
const int Meta::kEmpty = std::numeric_limits<int>::max();
size_t num_worker, num_server;
int server_push_nthread;
int server_pull_nthread;
std::mutex hash_mu_;
std::mutex map_mu_;
std::vector<std::mutex> push_mu_;
std::vector<std::mutex> pull_mu_;
std::vector<std::list<Message> > buffered_push_;
std::vector<std::list<Message> > buffered_pull_;

std::unordered_map<uint64_t, std::atomic<bool> > is_push_finished_;
std::atomic<int> thread_barrier_{0};
bool enable_profile_ = false;

std::unordered_map<uint64_t, uint64_t> hash_cache_;

Customer::Customer(int app_id, int customer_id, const Customer::RecvHandle& recv_handle)
    : app_id_(app_id), customer_id_(customer_id), recv_handle_(recv_handle) {
  Postoffice::Get()->AddCustomer(this);
  recv_thread_ = std::unique_ptr<std::thread>(new std::thread(&Customer::Receiving, this));
  // get the number of worker
  const char *val;
  val = Environment::Get()->find("DMLC_NUM_WORKER");
  num_worker = atoi(val);
  CHECK_GE(num_worker, 1);
  val = Environment::Get()->find("DMLC_NUM_SERVER");
  num_server = atoi(val);
  val = Environment::Get()->find("BYTEPS_SERVER_PUSH_NTHREADS");
  server_push_nthread = val ? atoi(val) : 1;
  val = Environment::Get()->find("BYTEPS_SERVER_PULL_NTHREADS");
  server_pull_nthread = val ? atoi(val) : 1;
  CHECK_GE(num_server, 1);
}

Customer::~Customer() {
  Postoffice::Get()->RemoveCustomer(this);
  Message msg;
  msg.meta.control.cmd = Control::TERMINATE;
  recv_queue_.Push(msg);
  recv_thread_->join();
}

int Customer::NewRequest(int recver) {
  std::lock_guard<std::mutex> lk(tracker_mu_);
  int num = Postoffice::Get()->GetNodeIDs(recver).size();
  tracker_.push_back(std::make_pair(num, 0));
  return tracker_.size() - 1;
}

void Customer::WaitRequest(int timestamp) {
  std::unique_lock<std::mutex> lk(tracker_mu_);
  tracker_cond_.wait(lk, [this, timestamp]{
      return tracker_[timestamp].first == tracker_[timestamp].second;
    });
}

int Customer::NumResponse(int timestamp) {
  std::lock_guard<std::mutex> lk(tracker_mu_);
  return tracker_[timestamp].second;
}

void Customer::AddResponse(int timestamp, int num) {
  std::lock_guard<std::mutex> lk(tracker_mu_);
  tracker_[timestamp].second += num;
}

bool Customer::IsValidPushpull(const Message &msg) {
  if (!msg.meta.control.empty()) return false;
  if (msg.meta.simple_app) return false;
  return true;
}

uint64_t Customer::GetKeyFromMsg(const Message &msg) { 
  CHECK(IsValidPushpull(msg)) << "Perform key derivation on an invalid message";
  CHECK_GT(msg.data.size(), 0) << "Invalid data message: msg.data.size() is 0";
  uint64_t key = 0;
  uint64_t coef = 1;
  for (unsigned int i = 0; i < msg.data[0].size(); ++i) {
    key += coef * (uint8_t) msg.data[0].data()[i];
    coef *= 256; // 256=2^8 (uint8_t)
  }
  return key;
}

uint64_t Customer::HashKey(uint64_t key) {
  std::lock_guard<std::mutex> lock(hash_mu_);
  if (hash_cache_.find(key) != hash_cache_.end()) {
    return hash_cache_[key];
  }
  auto str = std::to_string(key).c_str();
  uint64_t hash = 5381;
  int c;
  while ((c = *str)) { // hash(i) = hash(i-1) * 33 ^ str[i]
    hash = ((hash << 5) + hash) + c; 
    str++;
  }
  hash_cache_[key] = hash;
  return hash;
}

void Customer::ProcessPullRequest(int tid) {
  thread_barrier_.fetch_add(1);
  std::list<Message> pull_consumer;
  bool should_stop = false;
  std::unordered_map<uint64_t, size_t> pull_finished_cnt;

  while (!should_stop) {
    {
      std::lock_guard<std::mutex> lock(pull_mu_[tid]);
      CHECK_LT((unsigned int) tid, buffered_pull_.size());
      if (buffered_pull_[tid].size() != 0) {
        pull_consumer.splice(pull_consumer.end(), buffered_pull_[tid]);
        buffered_pull_[tid].clear();
      } 
    }
    auto it = pull_consumer.begin();
    while (it != pull_consumer.end()) {
      Message &msg = *it;
      if (!msg.meta.control.empty() && msg.meta.control.cmd == Control::TERMINATE) {
        if (pull_consumer.size() == 1) {
          should_stop = true;
          break;
        }
        continue; // should first finish the requests that are still in the buffer
      }
      CHECK(!msg.meta.push);
      uint64_t key = GetKeyFromMsg(msg);
      if (pull_finished_cnt.find(key) == pull_finished_cnt.end()) {
        pull_finished_cnt.emplace(key, 0);
      }
      // should have been inited
      auto push_tid = HashKey(key) % server_push_nthread;
      map_mu_.lock();
      auto is_finish = is_push_finished_[key].load();
      CHECK_NE(is_push_finished_.find(key), is_push_finished_.end()) << key;
      map_mu_.unlock();
      if (is_finish) {
        CHECK_LT(pull_finished_cnt[key], num_worker) << pull_finished_cnt[key];
        recv_handle_(msg);
        ++pull_finished_cnt[key];
        if ((size_t) pull_finished_cnt[key] == num_worker) {
          map_mu_.lock();
          is_push_finished_[key] = false;
          map_mu_.unlock();
          pull_finished_cnt[key] = 0;
        }
        it = pull_consumer.erase(it);
        if (enable_profile_) {
          Profile pdata = {key, msg.meta.sender, false, GetTimestampNow(), false};
          pdata_queue_.Push(pdata);
        }
        break;
      } else {
        ++it;
      }
    }
  }
}

void Customer::ProcessPushRequest(int tid) {
  thread_barrier_.fetch_add(1);
  std::unordered_map<uint64_t, int> push_finished_cnt;
  std::list<Message> push_consumer;
  bool should_stop = false;
  while (!should_stop) {
    {
      std::lock_guard<std::mutex> lock(push_mu_[tid]);
      CHECK_LT((unsigned int) tid, buffered_push_.size());
      if (buffered_push_[tid].size() != 0) {
        push_consumer.splice(push_consumer.end(), buffered_push_[tid]);
        buffered_push_[tid].clear();
      } 
    }
    auto it = push_consumer.begin();
    while (it != push_consumer.end()) {
      Message &msg = *it;
      if (!msg.meta.control.empty() && msg.meta.control.cmd == Control::TERMINATE) {
        if (push_consumer.size() == 1) {
          should_stop = true;
          break;
        }
        continue; // should first finish the requests that are still in the buffer
      }
      CHECK(msg.meta.push);
      uint64_t key = GetKeyFromMsg(msg);
      recv_handle_(msg);
      if (enable_profile_) {
        Profile pdata = {key, msg.meta.sender, true, GetTimestampNow(), false};
        pdata_queue_.Push(pdata);
      }
      it = push_consumer.erase(it);
      // init the push counter of each key
      if (push_finished_cnt.find(key) == push_finished_cnt.end()) {
        push_finished_cnt.emplace(key, 0);
      } 
      // we assume the init has already been handled by main thread
      ++push_finished_cnt[key];
      if ((size_t) push_finished_cnt[key] == num_worker) {
        std::lock_guard<std::mutex> lock(map_mu_);
        is_push_finished_[key] = true;
        push_finished_cnt[key] = 0;
      }
    }
  }
}

void Customer::ProcessProfileData() {
  LOG(INFO) << "profile thread is inited";
  bool profile_all = true; // default: profile all keys
  uint64_t key_to_profile = 0;
  const char *val;
  val = Environment::Get()->find("BYTEPS_SERVER_KEY_TO_PROFILE");
  if (val) {
    profile_all = false;
    key_to_profile = atoi(val);
  }
  std::fstream fout_;
  val = Environment::Get()->find("BYTEPS_SERVER_PROFILE_OUTPUT_PATH");
  fout_.open((val ? std::string(val) : "server_profile.json"), std::fstream::out);
  fout_ << "{\n";
  fout_ << "\t\"traceEvents\": [\n";
  bool is_init = true;
  while (true) {
    Profile pdata;
    pdata_queue_.WaitAndPop(&pdata);
    if (profile_all || key_to_profile==pdata.key) {
      if (!is_init) {
        fout_ << ",\n";
      } else {
        is_init = false;
      }
      fout_ << "\t\t" << "{\"name\": " << "\"" <<(pdata.is_push?"push":"pull") << "-" << pdata.sender << "\"" << ", "
            << "\"ph\": " << "\"" << (pdata.is_begin?"B":"E") << "\"" << ", "
            << "\"pid\": " << pdata.key << ", "
            << "\"tid\": " << pdata.key << ", "
            << "\"ts\": " << pdata.ts
            << "}";
    }
  }
  fout_ << "]\n";
  fout_ << "}";
  fout_.clear();
  fout_.flush();
  fout_.close();
  LOG(INFO) << "profile thread ended";
}

std::string Customer::GetTimestampNow() {
  std::chrono::microseconds us =
      std::chrono::duration_cast<std::chrono::microseconds >(std::chrono::system_clock::now().time_since_epoch());
  std::stringstream temp_stream;
  std::string ts_string;
  temp_stream << us.count();
  temp_stream >> ts_string;
  return ts_string;
}

void Customer::Receiving() {
  const char *val;
  val = CHECK_NOTNULL(Environment::Get()->find("DMLC_ROLE"));
  std::string role(val);
  bool is_server = role == "server";
  val = Environment::Get()->find("BYTEPS_ENABLE_SERVER_MULTITHREAD");
  bool is_server_multi_thread_enabled = val ? atoi(val) : true; // default enabled
  val = Environment::Get()->find("BYTEPS_ENABLE_ASYNC");
  bool enable_async = val ? atoi(val) : false;
  if (is_server && enable_async) {
    is_server_multi_thread_enabled = false;
  }
  // profiling
  val = Environment::Get()->find("BYTEPS_SERVER_ENABLE_PROFILE");
  enable_profile_ = val ? atoi(val) : false;
  std::thread* profile_thread = nullptr;
  if (enable_profile_ && is_server) {
    PS_VLOG(1) << "Enable server profiling";
    profile_thread = new std::thread(&Customer::ProcessProfileData, this);
  }

  if (is_server && is_server_multi_thread_enabled) { // server multi-thread
    PS_VLOG(1) << "Use separate thread to process pull requests from each worker.";

    // prepare push threads
    std::vector<std::thread *> push_thread;
    for (int i = 0; i < server_push_nthread; ++i) {
      std::list<Message> buf;
      buffered_push_.push_back(buf);
    }
    CHECK_EQ(buffered_push_.size(), (unsigned int) server_push_nthread);
    // prepare the mutexes before initing the threads
    std::vector<std::mutex> tmp_push_mu_list(server_push_nthread);
    push_mu_.swap(tmp_push_mu_list);
    // initiate push threads
    for (int i = 0; i < server_push_nthread; ++i) {
      auto t = new std::thread(&Customer::ProcessPushRequest, this, i);
      push_thread.push_back(t);
    }

    // prepare pull threads
    std::vector<std::thread *> pull_thread;
    for (int i = 0; i < server_pull_nthread; ++i) {
      std::list<Message> buf;
      buffered_pull_.push_back(buf);
    }
    CHECK_EQ(buffered_pull_.size(), (unsigned int) server_pull_nthread);
    // prepare the mutexes before initing the threads
    std::vector<std::mutex> tmp_pull_mu_list(server_pull_nthread);
    pull_mu_.swap(tmp_pull_mu_list);
    // initiate pull threads
    for (int i = 0; i < server_pull_nthread; ++i) {
      auto t = new std::thread(&Customer::ProcessPullRequest, this, i);
      pull_thread.push_back(t);
    }
    
    // wait until all threads have been inited
    int total_thread_num = server_push_nthread + server_pull_nthread;
    while (1) { 
      if (thread_barrier_.fetch_add(0) == total_thread_num) break;
      std::this_thread::sleep_for(std::chrono::nanoseconds(1000));
    }
    PS_VLOG(1) << "All push & pull threads inited, ready to process message ";

    std::unordered_map<uint64_t, std::set<int> > init_push_;

    while (true) {
      Message recv;
      recv_queue_.WaitAndPop(&recv);
      if (!recv.meta.control.empty() && recv.meta.control.cmd == Control::TERMINATE) {
        Message terminate_msg;
        terminate_msg.meta.control.cmd = Control::TERMINATE;
        for (int i = 0; i < server_push_nthread; ++i) {
          std::lock_guard<std::mutex> lock(push_mu_[i]);
          buffered_push_[i].push_back(terminate_msg);
        }
        for (int i = 0; i < server_pull_nthread; ++i) {
          std::lock_guard<std::mutex> lock(pull_mu_[i]);
          buffered_pull_[i].push_back(terminate_msg);
        }
        break;
      }
      if (!IsValidPushpull(recv)) {
        recv_handle_(recv);
        continue;
      }
      uint64_t key = GetKeyFromMsg(recv);

      if (init_push_[key].size() < num_worker) {
        CHECK(recv.meta.push) << key;
        // collect all push from each worker
        int sender = recv.meta.sender;
        CHECK_EQ(init_push_[key].find(sender), init_push_[key].end())
            << key << " " << sender;
        init_push_[key].insert(sender);
        recv_handle_(recv);
        // Reset the push flag, to guarantee that subsequent pulls are blocked.
        // We might be able to remove this, but just in case the compiler does not work as we expect.
        if (init_push_[key].size() == num_worker) {
          auto tid = HashKey(key) % server_push_nthread;
          std::lock_guard<std::mutex> lock(map_mu_);
          is_push_finished_[key] = false;
        }
        continue;
      }
      CHECK_EQ(init_push_[key].size(), num_worker);

      if (recv.meta.push) { // push: same key goes to same thread
        auto tid = HashKey(key) % server_push_nthread;
        std::lock_guard<std::mutex> lock(push_mu_[tid]);
        if (enable_profile_) {
          Profile pdata = {key, recv.meta.sender, true, GetTimestampNow(), true};
          pdata_queue_.Push(pdata);
        }
        buffered_push_[tid].push_back(recv);
      } else { // pull
        auto tid = HashKey(key) % server_pull_nthread;
        std::lock_guard<std::mutex> lock(pull_mu_[tid]);
        if (enable_profile_) {
          Profile pdata = {key, recv.meta.sender, false, GetTimestampNow(), true};
          pdata_queue_.Push(pdata);
        }
        buffered_pull_[tid].push_back(recv);
      }
    } // while
    // wait until the threads finish
    for (auto t : push_thread) t->join();
    for (auto t : pull_thread) t->join();
    if (profile_thread) profile_thread->join();
  } else { // original
    while (true) {
      Message recv;
      recv_queue_.WaitAndPop(&recv);
      if (!recv.meta.control.empty() 
            && recv.meta.control.cmd == Control::TERMINATE) {
        break;
      }
      recv_handle_(recv);
      if (!recv.meta.request) {
        std::lock_guard<std::mutex> lk(tracker_mu_);
        tracker_[recv.meta.timestamp].second++;
        tracker_cond_.notify_all();
      }
    }
  } // original
}

}  // namespace ps
