#pragma once

#include <vector>

#include "sql/operator/physical_operator.h"
#include "sql/parser/parse.h"
#include "common/log/log.h"

class Trx;
class UpdateStmt;

/**
 * @brief 物理算子，更新
 * @ingroup PhysicalOperator
 */
class UpdatePhysicalOperator : public PhysicalOperator
{
public:
  UpdatePhysicalOperator(
      Table *table, std::vector<std::unique_ptr<Expression>> &&values, std::vector<FieldMeta> &fields)
      : table_(table), values_(std::move(values))
  {
    for (FieldMeta &field : fields) {
      fields_.emplace_back(field.name());
    }
    tmp_record_data_ = (char *)malloc(table->table_meta().record_size());
  }

  virtual ~UpdatePhysicalOperator()
  {
    if (nullptr != tmp_record_data_) {
      free(tmp_record_data_);
    }
  }

  PhysicalOperatorType type() const override { return PhysicalOperatorType::UPDATE; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  // 查找待更新列的序号、偏移量、长度、类型
  RC find_target_columns();

  // 构造新的 Record
  RC construct_new_record(Record &old_record, Record &new_record);

  // 回滚时使用，从更新后 Record 构造出更新前的
  RC construct_old_record(Record &updated_record, Record &old_record);

  Tuple *current_tuple() override { return nullptr; }

private:
  Table                                   *table_ = nullptr;
  Trx                                     *trx_   = nullptr;
  std::vector<std::unique_ptr<Expression>> values_;
  std::vector<Value>                       raw_values_;
  std::vector<std::string>                 fields_;

  std::vector<int>       fields_id_;
  std::vector<FieldMeta> fields_meta_;
  char                  *tmp_record_data_ = nullptr;  // 用于存放新的Record的data

  // 存储已经更新过的行数据，用于回滚
  std::vector<RID>                old_rids_;
  std::vector<std::vector<Value>> old_values_;
  bool                            invalid_ = false;
};