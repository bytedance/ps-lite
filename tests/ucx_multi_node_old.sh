#!/bin/bash


function cleanup() {
    echo "kill all testing process of ps lite for user $USER"
    if [[ $EUID -ne 0 ]]; then
        pkill -9 -u $USER -f stress_test_benchmark
        pkill -9 -u $USER -f test_benchmark
    else
        pkill -9 -f stress_test_benchmark
        pkill -9 -f test_benchmark
    fi
    sleep 1
}
trap cleanup EXIT
# cleanup # cleanup on startup

set -ex

export DMLC_NUM_WORKER=${DMLC_NUM_WORKER:-1}
export DMLC_NUM_SERVER=${DMLC_NUM_SERVER:-1}

export UCX_SOCKADDR_CM_ENABLE=y
export UCX_USE_MT_MUTEX=y

# export LOG_DURATION=1

# export NODE_ONE_IP=10.0.0.1 # sched and server
# export NODE_TWO_IP=10.0.0.2 # worker

export DMLC_PS_ROOT_URI=${NODE_ONE_IP}  # try eth2
export BYTEPS_ORDERED_HOSTS=${NODE_ONE_IP},${NODE_TWO_IP}
export DMLC_NODE_HOST=${NODE_TWO_IP}  # by default it's remote
export UCX_RDMA_CM_SOURCE_ADDRESS=${NODE_TWO_IP}

export DMLC_PS_ROOT_PORT=19194     # scheduler's port (can random choose)
export DMLC_INTERFACE=eth0        # my RDMA interface
export DMLC_ENABLE_RDMA=0
export DMLC_ENABLE_UCX=1          # test ucx
# export UCX_TLS=all                # not working
# export UCX_TLS=ib,tcp           # working
#export UCX_TLS=ib,tcp,cuda_ipc,cuda_copy
#export UCX_TLS=ib,cuda_ipc,cuda_copy
# export UCX_MEMTYPE_CACHE=n
#export UCX_RNDV_SCHEME=put_zcopy
export BYTEPS_UCX_SHORT_THRESH=0

# export LOCAL_SIZE=1               # test ucx gdr
#export CUDA_VISIBLE_DEVICES=0,1,2,3
# export CUDA_VISIBLE_DEVICES=6,7
#export CUDA_VISIBLE_DEVICES=6,4,2,0
# export CUDA_VISIBLE_DEVICES=7,6,5,4,3,2,1,0
export CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES:-7,5,3,1,6,4,2,0}
# export CUDA_VISIBLE_DEVICES=2,6,4,0
# export CUDA_VISIBLE_DEVICES=6,0
# export CUDA_VISIBLE_DEVICES=1,7
#export UCX_IB_GPU_DIRECT_RDMA=no
export UCX_IB_GPU_DIRECT_RDMA=yes
# export UCX_MAX_RNDV_RAILS=4
export UCX_IB_NUM_PATHS=1
export UCX_IB_TRAFFIC_CLASS=236
# export UCX_NET_DEVICES=mlx5_2:1,mlx5_3:1
export UCX_IB_GID_INDEX=3
#export UCX_IB_IS_GLOBAL=y

export BYTEPS_ENABLE_IPC=0

export BYTEPS_NODE_ID=1

# export NUM_KEY_PER_SERVER=120
export NUM_KEY_PER_SERVER=40

if [ $# -eq 0 ] # no other args
then
#    export ENABLE_RECV_BUFFER=0
    # launch scheduler
    #echo "This is scheduler node."
    export BYTEPS_NODE_ID=0
    export DMLC_NODE_HOST=${NODE_ONE_IP}
    export UCX_RDMA_CM_SOURCE_ADDRESS=${NODE_ONE_IP}
    #DMLC_ROLE=scheduler ./test_benchmark &
    # launch server
    # 4M
    DMLC_ROLE=server ./test_benchmark 4096000
    # 40M
    # DMLC_ROLE=server ./test_benchmark 40960000
    exit 0
fi

if [ "$1" == "scheduler" ]; then
    # launch scheduler
    #echo "This is scheduler node."
    export BYTEPS_NODE_ID=0
    export DMLC_NODE_HOST=${NODE_ONE_IP}
    export UCX_RDMA_CM_SOURCE_ADDRESS=${NODE_ONE_IP}
    DMLC_ROLE=scheduler ./test_benchmark
    sleep infinity
    exit 0
fi

# launch worker, with 30MB data per push pull, 10000 rounds, push_then_pull mode
#DMLC_ROLE=worker BENCHMARK_NTHREAD=1 ./test_benchmark 30000000 10240 1
# push only
# DMLC_ROLE=worker BENCHMARK_NTHREAD=1 ./test_benchmark  4096000 1024000 2
# push_pull 4 M
DMLC_ROLE=worker BENCHMARK_NTHREAD=1 ./test_benchmark  4096000 1024000 1
# push_pull 40 M
# DMLC_ROLE=worker BENCHMARK_NTHREAD=1 ./test_benchmark  40960000 1024000 1

# for correctness test, use this following line and replace previous
# scheduler / server binary with ./test_correctness

# DMLC_ROLE=worker BENCHMARK_NTHREAD=1 ./test_correctness 30000000
