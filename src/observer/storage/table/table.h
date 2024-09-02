/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Meiyi & Wangyunlai on 2021/5/12.
//

#pragma once

#include "storage/table/table_meta.h"
#include "common/types.h"
#include "common/lang/span.h"
#include "common/lang/functional.h"

struct RID;
class Record;
class DiskBufferPool;
class RecordFileHandler;
class RecordFileScanner;
class ChunkFileScanner;
class ConditionFilter;
class DefaultConditionFilter;
class Index;
class IndexScanner;
class RecordDeleter;
class Trx;
class Db;


class Table
{
public:
  Table() = default;
  ~Table();

  RC create(Db *db, int32_t table_id, const char *path, const char *name, const char *base_dir,
      span<const AttrInfoSqlNode> attributes, StorageFormat storage_format);

  
  RC open(Db *db, const char *meta_file, const char *base_dir);

  RC remove(const char *name);
  
  RC make_record(int value_num, const Value *values, Record &record);

  
  RC insert_record(Record &record);
  RC delete_record(const Record &record);
  RC delete_record(const RID &rid);
  RC get_record(const RID &rid, Record &record);

  RC recover_insert_record(Record &record);

  RC update_record(Record &record, const char *attr_name, Value *value);
  RC update_record(Record &record, const std::vector<std::string> &attr_names, const std::vector<Value *> &values);
  RC update_record(Record &old_record, Record &new_record);
  // TODO refactor
  RC create_index(Trx *trx, const FieldMeta *field_meta, const char *index_name);

  RC get_record_scanner(RecordFileScanner &scanner, Trx *trx, ReadWriteMode mode);

  RC get_chunk_scanner(ChunkFileScanner &scanner, Trx *trx, ReadWriteMode mode);

  RecordFileHandler *record_handler() const { return record_handler_; }

  /**
   * @param rid
   * @param visitor
   * @return RC
   */
  RC visit_record(const RID &rid, function<bool(Record &)> visitor);

  RC write_text(int64_t &offset, int64_t length, const char *data);
  RC read_text(int64_t offset, int64_t length, char *data) const;

public:
  int32_t     table_id() const { return table_meta_.table_id(); }
  const char *name() const;

  Db *db() const { return db_; }

  const TableMeta &table_meta() const;

  RC sync();

private:
  RC insert_entry_of_indexes(const char *record, const RID &rid);
  RC delete_entry_of_indexes(const char *record, const RID &rid, bool error_on_not_exists);

private:
  RC init_record_handler(const char *base_dir);

public:
  Index *find_index(const char *index_name) const;
  Index *find_index_by_field(const char *field_name) const;

private:
  Db                *db_ = nullptr;
  string             base_dir_;
  TableMeta          table_meta_;
  DiskBufferPool    *data_buffer_pool_ = nullptr;  
  RecordFileHandler *record_handler_   = nullptr; 
  vector<Index *>    indexes_;
};
