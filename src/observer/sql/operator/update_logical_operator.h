#pragma once

#include <vector>

#include "sql/operator/logical_operator.h"
#include "sql/parser/parse_defs.h"
#include "sql/parser/value.h"
#include "storage/field/field.h"

class UpdateLogicalOperator : public LogicalOperator
{
public:
  UpdateLogicalOperator(Table *table, std::vector<std::unique_ptr<Expression>> &&values, std::vector<FieldMeta> &fields)
      : table_(table), values_(std::move(values)), fields_(fields)
  {}
  ~UpdateLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::UPDATE; }

  Table                                          *table() const { return table_; }
  const std::vector<std::unique_ptr<Expression>> &values() const { return values_; }
  std::vector<std::unique_ptr<Expression>>       &values() { return values_; }

  const std::vector<FieldMeta> &fields() const { return fields_; }
  std::vector<FieldMeta>       &fields() { return fields_; }

private:
  Table                                   *table_ = nullptr;
  std::vector<std::unique_ptr<Expression>> values_;
  std::vector<FieldMeta>                   fields_;
};