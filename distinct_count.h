/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef _DC_UDAF_H_
#define _DC_UDAF_H_

#include "udf/udf.h"

using namespace impala_udf;

// The following five method definitions are based on impala's udaf standard

void DistinctCountInit(FunctionContext* context, StringVal* val);

void DistinctCountUpdate(FunctionContext* context, const StringVal& arg1,StringVal* val);

void DistinctCountMerge(FunctionContext* context, const StringVal& src, StringVal* dst);

StringVal DistinctCountSerialize(FunctionContext* context, const StringVal& val);

StringVal DistinctCountFinalize(FunctionContext* context, const StringVal& val);

// Utility function for serialization to StringVal
template <typename T>
StringVal ToStringVal(FunctionContext* context, const T& val);

#endif