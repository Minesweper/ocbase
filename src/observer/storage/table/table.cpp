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
// Created by Meiyi & Wangyunlai on 2021/5/13.
//

#include <limits.h>
#include <string.h>
#include <algorithm>
#include "common/defs.h"
#include "common/lang/string.h"
#include "common/lang/span.h"
#include "common/lang/algorithm.h"
#include "common/log/log.h"
#include "common/global_context.h"
#include "storage/db/db.h"
#include "storage/buffer/disk_buffer_pool.h"
#include "storage/common/condition_filter.h"
#include "storage/common/meta_util.h"
#include "storage/index/bplus_tree_index.h"
#include "storage/index/index.h"
#include "storage/record/record_manager.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include "common/lang/defer.h"

Table::~Table()
{
  if (record_handler_ != nullptr) {
    delete record_handler_;
    record_handler_ = nullptr;
  }

  if (data_buffer_pool_ != nullptr) {
    data_buffer_pool_->close_file();
    data_buffer_pool_ = nullptr;
  }

  for (vector<Index *>::iterator it = indexes_.begin(); it != indexes_.end(); ++it) {
    Index *index = *it;
    delete index;
  }
  indexes_.clear();

  LOG_INFO("Table has been closed: %s", name());
}

RC Table::create(Db *db, int32_t table_id, const char *path, const char *name, const char *base_dir,
    span<const AttrInfoSqlNode> attributes, StorageFormat storage_format)
{
  if (table_id < 0) {
    LOG_WARN("invalid table id. table_id=%d, table_name=%s", table_id, name);
    return RC::INVALID_ARGUMENT;
  }

  if (common::is_blank(name)) {
    LOG_WARN("Name cannot be empty");
    return RC::INVALID_ARGUMENT;
  }
  LOG_INFO("Begin to create table %s:%s", base_dir, name);

  if (attributes.size() == 0) {
    LOG_WARN("Invalid arguments. table_name=%s, attribute_count=%d", name, attributes.size());
    return RC::INVALID_ARGUMENT;
  }

  RC rc = RC::SUCCESS;

  // 使用 table_name.table记录一个表的元数据
  // 判断表文件是否已经存在
  int fd = ::open(path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0) {
    if (EEXIST == errno) {
      LOG_ERROR("Failed to create table file, it has been created. %s, EEXIST, %s", path, strerror(errno));
      return RC::SCHEMA_TABLE_EXIST;
    }
    LOG_ERROR("Create table file failed. filename=%s, errmsg=%d:%s", path, errno, strerror(errno));
    return RC::IOERR_OPEN;
  }

  close(fd);

  // 创建文件
  const vector<FieldMeta> *trx_fields = db->trx_kit().trx_fields();
  if ((rc = table_meta_.init(table_id, name, trx_fields, attributes, storage_format)) != RC::SUCCESS) {
    LOG_ERROR("Failed to init table meta. name:%s, ret:%d", name, rc);
    return rc;  // delete table file
  }

  fstream fs;
  fs.open(path, ios_base::out | ios_base::binary);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open file for write. file name=%s, errmsg=%s", path, strerror(errno));
    return RC::IOERR_OPEN;
  }

  // 记录元数据到文件中
  table_meta_.serialize(fs);
  fs.close();

  db_       = db;
  base_dir_ = base_dir;

  string             data_file = table_data_file(base_dir, name);
  BufferPoolManager &bpm       = db->buffer_pool_manager();
  rc                           = bpm.create_file(data_file.c_str());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to create disk buffer pool of data file. file name=%s", data_file.c_str());
    return rc;
  }

  rc = init_record_handler(base_dir);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to create table %s due to init record handler failed.", data_file.c_str());
    // don't need to remove the data_file
    return rc;
  }

  LOG_INFO("Successfully create table %s:%s", base_dir, name);
  return rc;
}

RC Table::remove(const char *name){
  if (common::is_blank(name)) {
    LOG_WARN("Name cannot be empty");
    return RC::INVALID_ARGUMENT;
  }

  // remove the relevant index files
  for (auto idx : indexes_) {
    std::string index_file = table_index_file(base_dir_.c_str(), name, idx->index_meta().name());
    delete idx;  // the file will be closed in dtor
    if (0 != ::unlink(index_file.c_str())) {
      LOG_ERROR("Delete index file failed.");
      return RC::IOERR_ACCESS;
    }
  }
  indexes_.clear();

  // close the data file and clean relevant resources
  assert(nullptr != record_handler_);
  record_handler_->close();
  delete record_handler_;
  record_handler_ = nullptr;

  std::string data_file = table_data_file(base_dir_.c_str(), name);
  assert(nullptr != data_buffer_pool_);
  BufferPoolManager::instance().close_file(data_file.c_str());
  // NOTE: after bpm.close_file, the dbp has been released
  data_buffer_pool_ = nullptr;

  // remove the data file
  if (0 != ::unlink(data_file.c_str())) {
    LOG_ERROR("Delete data file failed.");
    return RC::IOERR_ACCESS;
  }

  // remove the meta file
  std::string meta_file = table_meta_file(base_dir_.c_str(), name);
  if (0 != ::unlink(meta_file.c_str())) {
    LOG_ERROR("Delete data file failed.");
    return RC::IOERR_ACCESS;
  }

  return RC::SUCCESS;
  // after we invoke the func, the dtor do nothing.
}

RC Table::open(Db *db, const char *meta_file, const char *base_dir)
{
  // 加载元数据文件
  fstream fs;
  string  meta_file_path = string(base_dir) + common::FILE_PATH_SPLIT_STR + meta_file;
  fs.open(meta_file_path, ios_base::in | ios_base::binary);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open meta file for read. file name=%s, errmsg=%s", meta_file_path.c_str(), strerror(errno));
    return RC::IOERR_OPEN;
  }
  if (table_meta_.deserialize(fs) < 0) {
    LOG_ERROR("Failed to deserialize table meta. file name=%s", meta_file_path.c_str());
    fs.close();
    return RC::INTERNAL;
  }
  fs.close();

  db_       = db;
  base_dir_ = base_dir;

  // 加载数据文件
  RC rc = init_record_handler(base_dir);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to open table %s due to init record handler failed.", base_dir);
    // don't need to remove the data_file
    return rc;
  }

  const int index_num = table_meta_.index_num();
  for (int i = 0; i < index_num; i++) {
    const IndexMeta *index_meta = table_meta_.index(i);
    std::vector<const FieldMeta *> field_metas;
    const std::vector<std::string> &field_name = index_meta->field();
    for (int j = 0; j < field_name.size(); i++) {
      const FieldMeta *field_meta = table_meta_.field(field_name[j].c_str());
      if (field_meta == nullptr) {
        LOG_ERROR("Found invalid index meta info which has a non-exists field. table=%s, index=%s, field=%s",
            name(),
            index_meta->name(),
            index_meta->field().data());
        // skip cleanup
        //  do all cleanup action in destructive Table function
        return RC::INTERNAL;
      }
      field_metas.emplace_back(field_meta);
    }

    BplusTreeIndex *index      = new BplusTreeIndex();
    string          index_file = table_index_file(base_dir, name(), index_meta->name());

    rc                         = index->open(this, index_file.c_str(), *index_meta, field_metas);
    if (rc != RC::SUCCESS) {
      delete index;
      LOG_ERROR("Failed to open index. table=%s, index=%s, file=%s, rc=%s",
                name(), index_meta->name(), index_file.c_str(), strrc(rc));
      // skip cleanup
      //  do all cleanup action in destructive Table function.
      return rc;
    }
    indexes_.push_back(index);
  }

  return rc;
}

RC Table::insert_record(Record &record)
{
  RC rc = RC::SUCCESS;
  rc    = record_handler_->insert_record(record.data(), table_meta_.record_size(), &record.rid());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Insert record failed. table name=%s, rc=%s", table_meta_.name(), strrc(rc));
    return rc;
  }

  rc = insert_entry_of_indexes(record.data(), record.rid());
  if (rc != RC::SUCCESS) {  // 可能出现了键值重复
    RC rc2 = delete_entry_of_indexes(record.data(), record.rid(), false /*error_on_not_exists*/);
    if (rc2 != RC::SUCCESS) {
      LOG_ERROR("Failed to rollback index data when insert index entries failed. table name=%s, rc=%d:%s",
                name(), rc2, strrc(rc2));
    }
    rc2 = record_handler_->delete_record(&record.rid());
    if (rc2 != RC::SUCCESS) {
      LOG_PANIC("Failed to rollback record data when insert index entries failed. table name=%s, rc=%d:%s",
                name(), rc2, strrc(rc2));
    }
  }
  return rc;
}

RC Table::visit_record(const RID &rid, function<bool(Record &)> visitor)
{
  return record_handler_->visit_record(rid, visitor);
}

RC Table::get_record(const RID &rid, Record &record)
{
  RC rc = record_handler_->get_record(rid, record);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to visit record. rid=%s, table=%s, rc=%s", rid.to_string().c_str(), name(), strrc(rc));
    return rc;
  }

  return rc;
}

RC Table::recover_insert_record(Record &record)
{
  RC rc = RC::SUCCESS;
  rc    = record_handler_->recover_insert_record(record.data(), table_meta_.record_size(), record.rid());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Insert record failed. table name=%s, rc=%s", table_meta_.name(), strrc(rc));
    return rc;
  }

  rc = insert_entry_of_indexes(record.data(), record.rid());
  if (rc != RC::SUCCESS) {  // 可能出现了键值重复
    RC rc2 = delete_entry_of_indexes(record.data(), record.rid(), false /*error_on_not_exists*/);
    if (rc2 != RC::SUCCESS) {
      LOG_ERROR("Failed to rollback index data when insert index entries failed. table name=%s, rc=%d:%s",
                name(), rc2, strrc(rc2));
    }
    rc2 = record_handler_->delete_record(&record.rid());
    if (rc2 != RC::SUCCESS) {
      LOG_PANIC("Failed to rollback record data when insert index entries failed. table name=%s, rc=%d:%s",
                name(), rc2, strrc(rc2));
    }
  }
  return rc;
}

const char *Table::name() const { return table_meta_.name(); }

const TableMeta &Table::table_meta() const { return table_meta_; }

RC Table::make_record(int value_num, const Value *values, Record &record)
{
  // 检查字段类型是否一致
  if (value_num + table_meta_.sys_field_num() != table_meta_.field_num()) {
    LOG_WARN("Input values don't match the table's schema, table name:%s", table_meta_.name());
    return RC::SCHEMA_FIELD_MISSING;
  }

  const int normal_field_start_index = table_meta_.sys_field_num();
  for (int i = 0; i < value_num; i++) {
    const FieldMeta *field = table_meta_.field(i + normal_field_start_index);
    const Value     &value = values[i];
    if (field->type() != value.attr_type()) {
      LOG_ERROR("Invalid value type. table name =%s, field name=%s, type=%d, but given=%d",
                table_meta_.name(), field->name(), field->type(), value.attr_type());
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
  }

  // 复制所有字段的值
  int   record_size = table_meta_.record_size();
  char *record_data = (char *)malloc(record_size);

  for (int i = 0; i < value_num; i++) {
    const FieldMeta *field    = table_meta_.field(i + normal_field_start_index);
    const Value     &value    = values[i];
    size_t           copy_len = field->len();
    if (field->type() == AttrType::CHARS) {
      const size_t data_len = value.length();
      if (copy_len > data_len) {
        copy_len = data_len + 1;
      }
    }
    memcpy(record_data + field->offset(), value.data(), copy_len);
  }

  record.set_data_owner(record_data, record_size);
  return RC::SUCCESS;
}

RC Table::init_record_handler(const char *base_dir)
{
  string data_file = table_data_file(base_dir, table_meta_.name());

  BufferPoolManager &bpm = db_->buffer_pool_manager();
  RC                 rc  = bpm.open_file(db_->log_handler(), data_file.c_str(), data_buffer_pool_);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to open disk buffer pool for file:%s. rc=%d:%s", data_file.c_str(), rc, strrc(rc));
    return rc;
  }

  record_handler_ = new RecordFileHandler(table_meta_.storage_format());

  rc = record_handler_->init(*data_buffer_pool_, db_->log_handler(), &table_meta_);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to init record handler. rc=%s", strrc(rc));
    data_buffer_pool_->close_file();
    data_buffer_pool_ = nullptr;
    delete record_handler_;
    record_handler_ = nullptr;
    return rc;
  }

  return rc;
}

RC Table::get_record_scanner(RecordFileScanner &scanner, Trx *trx, ReadWriteMode mode)
{
  RC rc = scanner.open_scan(this, *data_buffer_pool_, trx, db_->log_handler(), mode, nullptr);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("failed to open scanner. rc=%s", strrc(rc));
  }
  return rc;
}

RC Table::get_chunk_scanner(ChunkFileScanner &scanner, Trx *trx, ReadWriteMode mode)
{
  RC rc = scanner.open_scan_chunk(this, *data_buffer_pool_, db_->log_handler(), mode);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("failed to open scanner. rc=%s", strrc(rc));
  }
  return rc;
}

RC Table::delete_record(const RID &rid)
{
  RC     rc = RC::SUCCESS;
  Record record;
  rc = get_record(rid, record);
  if (OB_FAIL(rc)) {
    return rc;
  }

  return delete_record(record);
}

RC Table::delete_record(const Record &record)
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->delete_entry(record.data(), &record.rid());
    ASSERT(RC::SUCCESS == rc, 
           "failed to delete entry from index. table name=%s, index name=%s, rid=%s, rc=%s",
           name(), index->index_meta().name(), record.rid().to_string().c_str(), strrc(rc));
  }
  rc = record_handler_->delete_record(&record.rid());
  return rc;
}

RC Table::insert_entry_of_indexes(const char *record, const RID &rid)
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->insert_entry(record, &rid);
    if (rc != RC::SUCCESS) {
      break;
    }
  }
  return rc;
}

RC Table::delete_entry_of_indexes(const char *record, const RID &rid, bool error_on_not_exists)
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->delete_entry(record, &rid);
    if (rc != RC::SUCCESS) {
      if (rc != RC::RECORD_INVALID_KEY || !error_on_not_exists) {
        break;
      }
    }
  }
  return rc;
}

Index *Table::find_index(const char *index_name) const
{
  for (Index *index : indexes_) {
    if (0 == strcmp(index->index_meta().name(), index_name)) {
      return index;
    }
  }
  return nullptr;
}
Index *Table::find_index_by_field(const char *field_name) const
{
  const TableMeta &table_meta = this->table_meta();
  const IndexMeta *index_meta = table_meta.find_index_by_field(field_name);
  if (index_meta != nullptr) {
    return this->find_index(index_meta->name());
  }
  return nullptr;
}

RC Table::sync()
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->sync();
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to flush index's pages. table=%s, index=%s, rc=%d:%s",
          name(),
          index->index_meta().name(),
          rc,
          strrc(rc));
      return rc;
    }
  }

  rc = data_buffer_pool_->flush_all_pages();
  LOG_INFO("Sync table over. table=%s", name());
  return rc;
}


RC Table::write_text(int64_t &offset, int64_t length, const char *data)
{
  RC rc = RC::SUCCESS;
  rc    = text_buffer_pool_->append_data(offset, length, data);
  if (RC::SUCCESS != rc) {
    LOG_WARN("Failed to append text into disk_buffer_pool, rc=%s", strrc(rc));
    offset = -1;
    length = -1;
  }
  return rc;
}

RC Table::read_text(int64_t offset, int64_t length, char *data) const
{
  RC rc = RC::SUCCESS;
  if (0 > offset || 0 > length) {
    LOG_ERROR("Invalid param: text offset %ld, length %ld", offset, length);
    return RC::INVALID_ARGUMENT;
  }

  rc = text_buffer_pool_->get_data(offset, length, data);
  if (RC::SUCCESS != rc) {
    LOG_WARN("Failed to get text from disk_buffer_pool, rc=%s", strrc(rc));
  }
  return rc;
}

RC Table::update_record(Record &record, const char *attr_name, Value *value)
{
  std::vector<Value *> values;
  values.emplace_back(value);
  std::vector<std::string> attr_names;
  attr_names.emplace_back(attr_name);
  return update_record(record, attr_names, values);
}

RC Table::update_record(Record &record, const std::vector<std::string> &attr_names, const std::vector<Value *> &values)
{
  RC rc = RC::SUCCESS;
  if (attr_names.size() != values.size() || 0 == attr_names.size()) {
    rc = RC::INVALID_ARGUMENT;
    LOG_WARN("fields size not match values, or empty param");
    return rc;
  }

  int       field_offset   = -1;
  int       field_length   = -1;
  int       field_index    = -1;
  const int sys_field_num  = table_meta_.sys_field_num();
  const int user_field_num = table_meta_.field_num() - sys_field_num;

  char *old_data = record.data();                        // old_data不能释放，其指向的是frame中的内存
  char *data     = new char[table_meta_.record_size()];  // new_record->data

  DEFER(
    delete[] data;
    data = nullptr;
    record.set_data(old_data);
  );

  memcpy(data, old_data, table_meta_.record_size());

  for (size_t c_idx = 0; c_idx < attr_names.size(); c_idx++) {
    Value             *value     = values[c_idx];
    const std::string &attr_name = attr_names[c_idx];


    for (int i = 0; i < user_field_num; ++i) {
      const FieldMeta *field_meta = table_meta_.field(i + sys_field_num);
      const char      *field_name = field_meta->name();
      if (0 != strcmp(field_name, attr_name.c_str())) {
        continue;
      }
      AttrType attr_type  = field_meta->type();
      AttrType value_type = value->attr_type();
      if (value->is_null() && field_meta->nullable()) {
        // ok
      } else if (attr_type != value_type) {
        LOG_WARN("field type mismatch. table=%s, field=%s, field type=%d, value_type=%d",
            name(),
            field_meta->name(),
            attr_type,
            value_type);
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      }
      field_offset = field_meta->offset();
      field_length = field_meta->len();
      field_index  = i + sys_field_num;
      break;
    }
    if (field_length < 0 || field_offset < 0) {
      LOG_WARN("field not find ,field name = %s", attr_name.c_str());
      return RC::SCHEMA_FIELD_NOT_EXIST;
    }

    // 写入新的值
    const FieldMeta *null_field = table_meta_.null_field();
    common::Bitmap   new_null_bitmap(data + null_field->offset(), table_meta_.field_num());
    if (value->is_null()) {
      new_null_bitmap.set_bit(field_index);
    } else {
      new_null_bitmap.clear_bit(field_index);
      memcpy(data + field_offset, value->data(), field_length);
    }
  }
  record.set_data(data);  // 谁来管理old_data呢？

  rc = delete_entry_of_indexes(old_data, record.rid(), false);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to delete indexes of record (rid=%d.%d). rc=%d:%s",
        record.rid().page_num,
        record.rid().slot_num,
        rc,
        strrc(rc));
    return rc;
  }

  rc = insert_entry_of_indexes(record.data(), record.rid());
  if (rc != RC::SUCCESS) {  // 可能出现了键值重复
    RC rc2 = insert_entry_of_indexes(old_data, record.rid());
    if (rc2 != RC::SUCCESS) {
      //sql_debug("Failed to rollback index after insert index failed, rc=%s", strrc(rc2));
      LOG_ERROR("Failed to rollback index data when insert index entries failed. table name=%s, rc=%d:%s",
          name(),
          rc2,
          strrc(rc2));
      return rc2;
    }
    return rc;  // 插入新的索引失败
  }

  rc = record_handler_->update_record(&record);
  if (rc != RC::SUCCESS) {
    LOG_ERROR(
        "Failed to update record (rid=%d.%d). rc=%d:%s", record.rid().page_num, record.rid().slot_num, rc, strrc(rc));
    return rc;
  }

  record.set_data(old_data);
  return rc;
}

RC Table::update_record(Record &old_record, Record &new_record)
{
  RC rc = RC::SUCCESS;

  rc = delete_entry_of_indexes(old_record.data(), old_record.rid(), false);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to delete indexes of record (rid=%d.%d). rc=%d:%s",
        old_record.rid().page_num,
        old_record.rid().slot_num,
        rc,
        strrc(rc));
    return rc;
  }

  rc = insert_entry_of_indexes(new_record.data(), new_record.rid());
  if (rc != RC::SUCCESS) {  // 可能出现了键值重复
    RC rc2 = insert_entry_of_indexes(old_record.data(), old_record.rid());
    if (rc2 != RC::SUCCESS) {
      
      LOG_ERROR("Failed to rollback index data when insert index entries failed. table name=%s, rc=%d:%s",
          name(),
          rc2,
          strrc(rc2));
      return rc2;
    }
    return rc;  // 插入新的索引失败
  }

  rc = record_handler_->update_record(&new_record);
  if (rc != RC::SUCCESS) {
    LOG_ERROR(
        "Failed to update record (rid=%d.%d). rc=%d:%s", new_record.rid().page_num, new_record.rid().slot_num, rc, strrc(rc));
    return rc;
  }

  return rc;
}

RC Table::create_index(Trx *trx, bool unique, const std::vector<const FieldMeta *> &field_metas, const char *index_name)
{
  if (common::is_blank(index_name) || field_metas.empty()) {
    LOG_INFO("Invalid input arguments, table name is %s, index_name is blank or attribute_name is blank", name());
    return RC::INVALID_ARGUMENT;
  }

  IndexMeta new_index_meta;
  RC        rc = new_index_meta.init(index_name, unique, field_metas);
  if (rc != RC::SUCCESS) {
    std::string field_names = field_metas[0]->name();
    for (int i = 0; i < field_metas.size(); i++) {
      field_names += ", ";
      field_names += field_metas[i]->name();
    }
    LOG_INFO("Failed to init IndexMeta in table:%s, index_name:%s, field_name:%s", 
             name(), index_name, field_names.c_str());
    return rc;
  }

  std::vector<int> field_ids;
  for (size_t i = 0; i < field_metas.size(); i++) {
    const FieldMeta *field_meta = field_metas[i];
    int              field_id   = 0;
    for (FieldMeta field : *table_meta_.field_metas()) {
      if (0 == strcmp(field.name(), field_meta->name())) {
        field_ids.emplace_back(field_id);
        break;
      }
      field_id++;
    }
  }
  if (field_ids.size() != field_metas.size()) {
    rc = RC::VARIABLE_NOT_VALID;
    LOG_ERROR("Failed to find column_id for all index_fields, column_id size:%d, index_field size:%d", 
                field_ids.size(), field_metas.size());
    return rc;
  }

  
  BplusTreeIndex *index      = new BplusTreeIndex();
  std::string     index_file = table_index_file(base_dir_.c_str(), name(), index_name);
  rc                         = index->create(this, index_file.c_str(), unique, new_index_meta, field_ids, field_metas);
  if (rc != RC::SUCCESS) {
    delete index;
    LOG_ERROR("Failed to create bplus tree index. file name=%s, rc=%d:%s", index_file.c_str(), rc, strrc(rc));
    return rc;
  }

  // 遍历当前的所有数据，插入这个索引
  RecordFileScanner scanner;
  rc = get_record_scanner(scanner, trx, ReadWriteMode::READ_ONLY);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create scanner while creating index. table=%s, index=%s, rc=%s", name(), index_name, strrc(rc));
    return rc;
  }

  Record record;
  while (scanner.has_next()) {
    rc = scanner.next(record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to scan records while creating index. table=%s, index=%s, rc=%s", name(), index_name, strrc(rc));
      return rc;
    }
    rc = index->insert_entry(record.data(), &record.rid());
    if (rc != RC::SUCCESS) {
      // TODO: 插入失败，应该删除索引
      LOG_WARN("failed to insert record into index while creating index. table=%s, index=%s, rc=%s",
          name(), index_name, strrc(rc));
      return rc;
    }
  }
  scanner.close_scan();
  LOG_INFO("inserted all records into new index. table=%s, index=%s", name(), index_name);

  indexes_.push_back(index);

  /// 接下来将这个索引放到表的元数据中
  TableMeta new_table_meta(table_meta_);
  rc = new_table_meta.add_index(new_index_meta);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to add index (%s) on table (%s). error=%d:%s", index_name, name(), rc, strrc(rc));
    return rc;
  }

  /// 内存中有一份元数据，磁盘文件也有一份元数据。修改磁盘文件时，先创建一个临时文件，写入完成后再rename为正式文件
  /// 这样可以防止文件内容不完整
  // 创建元数据临时文件
  std::string  tmp_file = table_meta_file(base_dir_.c_str(), name()) + ".tmp";
  std::fstream fs;
  fs.open(tmp_file, std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open file for write. file name=%s, errmsg=%s", tmp_file.c_str(), strerror(errno));
    return RC::IOERR_OPEN;  // 创建索引中途出错，要做还原操作
  }
  if (new_table_meta.serialize(fs) < 0) {
    LOG_ERROR("Failed to dump new table meta to file: %s. sys err=%d:%s", tmp_file.c_str(), errno, strerror(errno));
    return RC::IOERR_WRITE;
  }
  fs.close();

  // 覆盖原始元数据文件
  std::string meta_file = table_meta_file(base_dir_.c_str(), name());
  int         ret       = rename(tmp_file.c_str(), meta_file.c_str());
  if (ret != 0) {
    LOG_ERROR("Failed to rename tmp meta file (%s) to normal meta file (%s) while creating index (%s) on table (%s). "
              "system error=%d:%s",
        tmp_file.c_str(),
        meta_file.c_str(),
        index_name,
        name(),
        errno,
        strerror(errno));
    return RC::IOERR_WRITE;
  }

  table_meta_.swap(new_table_meta);

  LOG_INFO("Successfully added a new index (%s) on the table (%s)", index_name, name());
  return rc;
}