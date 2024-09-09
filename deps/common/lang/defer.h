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
// Created by Wangyunlai on 2022/07/08.
//

#pragma once

#include <functional>
namespace common {

class DeferHelper
{
public:
  DeferHelper(const std::function<void()> defer) : defer_(std::move(defer)) {}

  ~DeferHelper()
  {
    if (defer_) {
      defer_();
    }
  }

private:
  const std::function<void()> defer_;
};

}  // namespace common

#define _SCOPE_UNIQUE_NAME(B, C) B##C

#define SCOPE_UNIQUE_NAME(B, C) _SCOPE_UNIQUE_NAME(B, C)

#define SCOPE_UNIQUE_NAME2(B) SCOPE_UNIQUE_NAME(B, __COUNTER__)

#define DEFER(code) common::DeferHelper SCOPE_UNIQUE_NAME(defer_helper_)([&]() { code; })
