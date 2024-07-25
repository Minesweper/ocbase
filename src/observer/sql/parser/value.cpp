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
// Created by WangYunlai on 2023/06/28.
//

#include "sql/parser/value.h"
#include "common/lang/comparator.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include <sstream>
#include <string>

int  date::getday() { return d; }
int  date::getmonth() { return m; }
int  date::getyear() { return y; }
std::string date::date_to_s() { 
    std::string tp1(itoa(y));
  std::string tp2(itoa(m));
  std::string tp3(itoa(d));
  std::string ans = tp1 + "-" + tp2 + "-" + tp3;
  return ans;
}
date::date(const date &ot)
{
  y = ot.y;
  m = ot.m;
  d = ot.d;
}
date s_to_date(std::string str1)
  {
  int pos = str1.find('-');
  std::string a1 = str1.substr(0, pos);
  std::string str2 = str1.substr(pos + 1,str1.length());
  pos = str2.find('-');
  std::string a2 = str2.substr(0, pos);
  std::string a3 = str2.substr(pos + 1, str2.length());
  int yy  = atoi(a1.c_str());
  int mm  = atoi(a2.c_str());
  int dd  = atoi(a3.c_str());
  if (yy >= 2039 || yy < 1970)
    return date(0, 0, 0);
  if (mm > 12 || mm < 1)
    return date(0, 0, 0);
  if (dd > 31 || dd < 1)
    return date(0, 0, 0);
  if (yy == 2038 && mm > 2)
    return date(0, 0, 0);

  if ((!(mm % 2)) && (mm < 7)) {
    if (mm == 2) {
      if (yy % 4 == 0) {
        if (dd > 29)
          return date(0, 0, 0);
      } else {
        if (dd > 28)
          return date(0, 0, 0);
      }
    } else {
      if (dd > 30)
        return date(0, 0, 0);
    }
  } else if ((mm % 2) && (mm > 8)) {
    if (dd > 30)
      return date(0, 0, 0);
  }
  return date(yy, mm, dd);
}

const char *ATTR_TYPE_NAME[] = {"undefined", "chars", "ints", "floats", "booleans", "dates"};

const char *attr_type_to_string(AttrType type)
{
  if (type >= AttrType::UNDEFINED && type <= AttrType::FLOATS) {
    return ATTR_TYPE_NAME[static_cast<int>(type)];
  }
  if (type == AttrType::DATES)
    return ATTR_TYPE_NAME[static_cast<int>(type)];
  return "unknown";
}
AttrType attr_type_from_string(const char *s)
{
  for (unsigned int i = 0; i < sizeof(ATTR_TYPE_NAME) / sizeof(ATTR_TYPE_NAME[0]); i++) {
    if (0 == strcmp(ATTR_TYPE_NAME[i], s)) {
      return (AttrType)i;
    }
  }
  return AttrType::UNDEFINED;
}

Value::Value(int val) { set_int(val); }

Value::Value(float val) { set_float(val); }

Value::Value(bool val) { set_boolean(val); }

Value::Value(date dt) { set_date(dt); }

Value::Value(const char *s, int len /*= 0*/) { set_string(s, len); }

void Value::set_data(char *data, int length)
{
  switch (attr_type_) {
    case AttrType::CHARS: {
      set_string(data, length);
    } break;
    case AttrType::INTS: {
      num_value_.int_value_ = *(int *)data;
      length_               = length;
    } break;
    case AttrType::FLOATS: {
      num_value_.float_value_ = *(float *)data;
      length_                 = length;
    } break;
    case AttrType::BOOLEANS: {
      num_value_.bool_value_ = *(int *)data != 0;
      length_                = length;
    } break;
    case AttrType::DATES: {
      date tp = s_to_date(data);
      if (tp.getday() == 0)
        LOG_WARN("error date!");
      date_value_ = tp;
      length_     = length;
    } break;
    default: {
      LOG_WARN("unknown data type: %d", attr_type_);
    } break;
  }
}
void Value::set_int(int val)
{
  attr_type_            = AttrType::INTS;
  num_value_.int_value_ = val;
  length_               = sizeof(val);
}

void Value::set_float(float val)
{
  attr_type_              = AttrType::FLOATS;
  num_value_.float_value_ = val;
  length_                 = sizeof(val);
}
void Value::set_boolean(bool val)
{
  attr_type_             = AttrType::BOOLEANS;
  num_value_.bool_value_ = val;
  length_                = sizeof(val);
}
void Value::set_string(const char *s, int len /*= 0*/)
{
  attr_type_ = AttrType::CHARS;
  if (len > 0) {
    len = strnlen(s, len);
    str_value_.assign(s, len);
  } else {
    str_value_.assign(s);
  }
  length_ = str_value_.length();
}
void Value::set_date(const date &dt)
{
  attr_type_  = AttrType::DATES;
  date_value_ = dt;
  length_     = sizeof(date);
}
void Value::set_value(const Value &value)
  {
  switch (value.attr_type_) {
    case AttrType::INTS: {
      set_int(value.get_int());
    } break;
    case AttrType::FLOATS: {
      set_float(value.get_float());
    } break;
    case AttrType::CHARS: {
      set_string(value.get_string().c_str());
    } break;
    case AttrType::BOOLEANS: {
      set_boolean(value.get_boolean());
    } break;
    case AttrType::DATES: {
      if (value.get_date().getday() == 0)
        ASSERT(false,"got an error date");
      else set_date(value.get_date());
    } break;
    case AttrType::UNDEFINED: {
      ASSERT(false, "got an invalid value type");
    } break;
  }
}

const char *Value::data() const
{
  switch (attr_type_) {
    case AttrType::CHARS: {
      return str_value_.c_str();
    } break;
    case AttrType::DATES: { 
        return date_value_.date_to_s().c_str();
      break;
    }
    default: {
      return (const char *)&num_value_;
    } break;
  }
}

std::string Value::to_string() const
{
  std::stringstream os;
  switch (attr_type_) {
    case AttrType::INTS: {
      os << num_value_.int_value_;
    } break;
    case AttrType::FLOATS: {
      os << common::double_to_str(num_value_.float_value_);
    } break;
    case AttrType::BOOLEANS: {
      os << num_value_.bool_value_;
    } break;
    case AttrType::CHARS: {
      os << str_value_;
    } break;
    case AttrType::DATES: {
      os << date_value_.date_to_s();
    } break;
    default: {
      LOG_WARN("unsupported attr type: %d", attr_type_);
    } break;
  }
  return os.str();
}

int Value::compare(const Value &other) const
{
  if (this->attr_type_ == other.attr_type_) {
    switch (this->attr_type_) {
      case AttrType::INTS: {
        return common::compare_int((void *)&this->num_value_.int_value_, (void *)&other.num_value_.int_value_);
      } break;
      case AttrType::FLOATS: {
        return common::compare_float((void *)&this->num_value_.float_value_, (void *)&other.num_value_.float_value_);
      } break;
      case AttrType::CHARS: {
        return common::compare_string((void *)this->str_value_.c_str(),
            this->str_value_.length(),
            (void *)other.str_value_.c_str(),
            other.str_value_.length());
      } break;
      case AttrType::BOOLEANS: {
        return common::compare_int((void *)&this->num_value_.bool_value_, (void *)&other.num_value_.bool_value_);
      } break;
      case AttrType::DATES: {
        if (this->date_value_ > other.date_value_)
          return 1;
        else if (this->date_value_ == other.date_value_)
          return 0;
        else
          return -1;
      } break;
      default: {
        LOG_WARN("unsupported type: %d", this->attr_type_);
      }
    }
  } else if (this->attr_type_ == AttrType::INTS && other.attr_type_ == AttrType::FLOATS) {
    float this_data = this->num_value_.int_value_;
    return common::compare_float((void *)&this_data, (void *)&other.num_value_.float_value_);
  } else if (this->attr_type_ == AttrType::FLOATS && other.attr_type_ == AttrType::INTS) {
    float other_data = other.num_value_.int_value_;
    return common::compare_float((void *)&this->num_value_.float_value_, (void *)&other_data);
  }
  LOG_WARN("not supported");
  return -1;  // TODO return rc?
}

int Value::get_int() const
{
  switch (attr_type_) {
    case AttrType::CHARS: {
      try {
        return (int)(std::stol(str_value_));
      } catch (std::exception const &ex) {
        LOG_TRACE("failed to convert string to number. s=%s, ex=%s", str_value_.c_str(), ex.what());
        return 0;
      }
    }
    case AttrType::INTS: {
      return num_value_.int_value_;
    }
    case AttrType::FLOATS: {
      return (int)(num_value_.float_value_);
    }
    case AttrType::BOOLEANS: {
      return (int)(num_value_.bool_value_);
    }
    case AttrType::DATES: {
      LOG_TRACE("failed to convert date to number.");
      return 0;
    }
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);
      return 0;
    }
  }
  return 0;
}

float Value::get_float() const
{
  switch (attr_type_) {
    case AttrType::CHARS: {
      try {
        return std::stof(str_value_);
      } catch (std::exception const &ex) {
        LOG_TRACE("failed to convert string to float. s=%s, ex=%s", str_value_.c_str(), ex.what());
        return 0.0;
      }
    } break;
    case AttrType::INTS: {
      return float(num_value_.int_value_);
    } break;
    case AttrType::FLOATS: {
      return num_value_.float_value_;
    } break;
    case AttrType::BOOLEANS: {
      return float(num_value_.bool_value_);
    } break;
    case AttrType::DATES: {
      LOG_TRACE("failed to convert date to float.");
      return 0.0;
    } break;
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);
      return 0;
    }
  }
  return 0;
}

std::string Value::get_string() const { return this->to_string(); }

bool Value::get_boolean() const
{
  switch (attr_type_) {
    case AttrType::CHARS: {
      try {
        float val = std::stof(str_value_);
        if (val >= EPSILON || val <= -EPSILON) {
          return true;
        }

        int int_val = std::stol(str_value_);
        if (int_val != 0) {
          return true;
        }

        return !str_value_.empty();
      } catch (std::exception const &ex) {
        LOG_TRACE("failed to convert string to float or integer. s=%s, ex=%s", str_value_.c_str(), ex.what());
        return !str_value_.empty();
      }
    } break;
    case AttrType::INTS: {
      return num_value_.int_value_ != 0;
    } break;
    case AttrType::FLOATS: {
      float val = num_value_.float_value_;
      return val >= EPSILON || val <= -EPSILON;
    } break;
    case AttrType::BOOLEANS: {
      return num_value_.bool_value_;
    } break;
    case AttrType::DATES: {
      LOG_TRACE("failed to convert date to boolean.");
      return false;
    } break;
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);
      return false;
    }
  }
  return false;
}
