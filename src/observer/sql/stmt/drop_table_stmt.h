#pragma once

#include "sql/parser/parse_defs.h"
#include "sql/stmt/stmt.h"

class DropTableStmt :public Stmt {
public:
  StmtType type() const override { return StmtType::DROP_TABLE; }
  virtual ~DropTableStmt() = default;
  DropTableStmt(const std::string &table_name):table_name_(table_name){};
  static RC create(Db *db, const DropTableSqlNode &drop_table, Stmt *&stmt);
  const std::string &table_name() const { return table_name_; }


private:
  std::string table_name_;
};