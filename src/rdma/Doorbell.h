#pragma once

#include <infiniband/verbs.h>
#include <infiniband/verbs_exp.h>

#include <cstring>

#include "common/Type.h"
#include "rdma/RdmaBatch.h"
#include "util/Macros.h"

struct WriteRecordBatch {
    ibv_send_wr wrs[2];
    ibv_sge sges[2];
    ibv_send_wr* bad_wr;
    node_id_t nid;

    WriteRecordBatch() {}

    void WriteColumns(void* laddr, size_t sz, PoolPtr raddr) {
        // ASSERT(sz != 64, "Invalid size");
        this->nid = NodeId(raddr);
        ibv_sge& sge = sges[0];
        ibv_send_wr& wr = wrs[0];

        sge.addr = (uint64_t)laddr;
        sge.length = (uint32_t)sz;

        std::memset(&wr, 0, sizeof(wr));
        wr.wr.rdma.remote_addr = NodeAddress(raddr);
        wr.opcode = IBV_WR_RDMA_WRITE;
        // Not signal this element
        wr.send_flags = 0;

        wr.num_sge = 1;
        wr.sg_list = &sge;
    }

    void WriteSubversions(void* laddr, size_t sz, PoolPtr raddr) {
        // ASSERT(sz != 64, "Invalid size");
        this->nid = NodeId(raddr);
        ibv_sge& sge = sges[1];
        ibv_send_wr& wr = wrs[1];

        sge.addr = (uint64_t)laddr;
        sge.length = (uint32_t)sz;

        std::memset(&wr, 0, sizeof(wr));
        wr.wr.rdma.remote_addr = NodeAddress(raddr);
        wr.opcode = IBV_WR_RDMA_WRITE;
        // The last request should be signaled
        wr.send_flags = IBV_SEND_SIGNALED;

        if (sz < rdma::RdmaConfig::MaxDoorbellInlineSize) {
            wr.send_flags |= IBV_SEND_INLINE;
        }

        wr.num_sge = 1;
        wr.sg_list = &sge;
    }
};

struct LockReadBatch {
    struct ibv_exp_send_wr wrs[2];
    struct ibv_sge sges[2];
    ibv_exp_send_wr* bad_wr;
    node_id_t nid;

    LockReadBatch() {}

    void LockColumns(void* laddr, uint64_t compare, uint64_t swap, uint64_t mask, PoolPtr raddr) {
        this->nid = NodeId(raddr);
        ibv_sge& sge = sges[0];
        ibv_exp_send_wr& wr = wrs[0];

        memset(&sge, 0, sizeof(sge));
        sge.addr = (uint64_t)laddr;
        sge.length = sizeof(uint64_t);

        memset(&wr, 0, sizeof(wr));
        wr.sg_list = &sge;
        wr.num_sge = 1;

        wr.exp_opcode = IBV_EXP_WR_EXT_MASKED_ATOMIC_CMP_AND_SWP;
        wr.exp_send_flags = IBV_EXP_SEND_EXT_ATOMIC_INLINE;

        wr.ext_op.masked_atomics.log_arg_sz = 3;
        wr.ext_op.masked_atomics.remote_addr = NodeAddress(raddr);

        auto& op = wr.ext_op.masked_atomics.wr_data.inline_data.op.cmp_swap;
        op.compare_val = compare;
        op.swap_val = swap;

        op.compare_mask = mask;
        op.swap_mask = mask;
    }

    void ReadRecord(void* laddr, size_t sz, PoolPtr raddr) {
        this->nid = NodeId(raddr);
        ibv_sge& sge = sges[1];
        ibv_exp_send_wr& wr = wrs[1];

        memset(&sge, 0, sizeof(sge));
        sge.addr = (uint64_t)laddr;
        sge.length = (uint32_t)sz;
        // We will fill in the lkey when qp is known

        memset(&wr, 0, sizeof(wr));
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.exp_opcode = IBV_EXP_WR_RDMA_READ;
        wr.exp_send_flags = IBV_EXP_SEND_SIGNALED;
        wr.wr.rdma.remote_addr = NodeAddress(raddr);
    }
};