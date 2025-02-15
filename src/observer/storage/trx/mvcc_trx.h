/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2023/04/24.
//

#pragma once

#include "common/lang/vector.h"
#include "storage/trx/trx.h"
#include "storage/trx/mvcc_trx_log.h"

class CLogManager;
class LogHandler;
class MvccTrxLogHandler;

class MvccTrxKit : public TrxKit
{
public:
  MvccTrxKit() = default;
  virtual ~MvccTrxKit();

  RC                       init() override;
  const vector<FieldMeta> *trx_fields() const override;

  Trx *create_trx(LogHandler &log_handler) override;
  Trx *create_trx(LogHandler &log_handler, int32_t trx_id) override;
  void destroy_trx(Trx *trx) override;

  
  Trx *find_trx(int32_t trx_id) override;
  void all_trxes(vector<Trx *> &trxes) override;

  LogReplayer *create_log_replayer(Db &db, LogHandler &log_handler) override;

public:
  int32_t next_trx_id();

public:
  int32_t max_trx_id() const;

private:
  vector<FieldMeta> fields_;  

  atomic<int32_t> current_trx_id_{0};

  common::Mutex lock_;
  vector<Trx *> trxes_;
};

/**
 * TODO 
 */
class MvccTrx : public Trx
{
public:
  
  MvccTrx(MvccTrxKit &trx_kit, LogHandler &log_handler);
  MvccTrx(MvccTrxKit &trx_kit, LogHandler &log_handler, int32_t trx_id);  // used for recover
  virtual ~MvccTrx();

  RC insert_record(Table *table, Record &record) override;
  RC delete_record(Table *table, Record &record) override;

  
  RC visit_record(Table *table, Record &record, ReadWriteMode mode) override;

  RC start_if_need() override;
  RC commit() override;
  RC rollback() override;

  RC redo(Db *db, const LogEntry &log_entry) override;

  int32_t id() const override { return trx_id_; }

private:
  RC   commit_with_trx_id(int32_t commit_id);
  void trx_fields(Table *table, Field &begin_xid_field, Field &end_xid_field) const;

private:
  static const int32_t MAX_TRX_ID = numeric_limits<int32_t>::max();

private:
  // using OperationSet = unordered_set<Operation, OperationHasher, OperationEqualer>;
  using OperationSet = vector<Operation>;

  MvccTrxKit       &trx_kit_;
  MvccTrxLogHandler log_handler_;
  int32_t           trx_id_     = -1;
  bool              started_    = false;
  bool              recovering_ = false;
  OperationSet      operations_;
};
