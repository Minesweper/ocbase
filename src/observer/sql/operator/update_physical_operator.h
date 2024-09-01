#pragma once

#include <vector>

#include "sql/operator/physical_operator.h"
#include "sql/parser/parse.h"
#include "common/log/log.h"

class Trx;
class UpdateStmt;


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


  RC find_target_columns();


  RC construct_new_record(Record &old_record, Record &new_record);


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
  char                  *tmp_record_data_ = nullptr;  

  
  std::vector<RID>                old_rids_;
  std::vector<std::vector<Value>> old_values_;
  bool                            invalid_ = false;
};