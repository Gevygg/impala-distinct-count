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
#include "distinct_count.h"
#include <ctype.h>
#include <assert.h>
#include <sstream>
#include <iostream>
#include <set>

using namespace impala_udf;
using namespace std;

template <typename T>
StringVal ToStringVal(FunctionContext* context, const T& val) {
  stringstream ss;
  ss << val;
  string str = ss.str();
  StringVal string_val(context, str.size());
  memcpy(string_val.ptr, str.c_str(), str.size());
  return string_val;
}

template <>
StringVal ToStringVal<DoubleVal>(FunctionContext* context, const DoubleVal& val) {
  if (val.is_null) return StringVal::null();
  return ToStringVal(context, val.val);
}

// Gets the number of characters specified in the string
unsigned int c_in_str(uint8_t* str,int len,char ch)
{
	unsigned int count = 0;
	
	for(int i=0;i<len;i++){
		if (*str == ch)
			count++;
		str++;
	}

	return count;
}

// format conversion : int to string
string ltos(int l)
{
	ostringstream os;
	os << l;
	string result;
	istringstream is(os.str());
	is >> result;
	return result;

}

static const StringVal DEFAULT_STRING_CONCAT_DELIM((uint8_t*)", ", 2);

// Initialization method
void DistinctCountInit(FunctionContext* context, StringVal* val) {
  val->is_null = true;
  memset(val->ptr, 0, val->len);
}

// Cache all column values,split with ',' in memory
void DistinctCountUpdate(FunctionContext* context, const StringVal& str,StringVal* result) {
  if (str.is_null) return;
  if (result->is_null) {
    uint8_t* copy = context->Allocate(str.len);
    if (copy == NULL) return;
    memcpy(copy, str.ptr, str.len);
    *result = StringVal(copy, str.len);
    return;
  }
  int new_size = result->len + 1 + str.len;
  result->ptr = context->Reallocate(result->ptr, new_size);
  if (result->ptr == NULL) {
    *result = StringVal::null();
    return;
  }
  memcpy(result->ptr + result->len,",", 1);
  result->len += 1;
  memcpy(result->ptr + result->len, str.ptr, str.len);
  result->len += str.len;
}

// merge all result,reduce subset 
void DistinctCountMerge(FunctionContext* context, const StringVal& src, StringVal* dst) {
  if (src.is_null) return;
  DistinctCountUpdate(context, src, dst);
}


StringVal DistinctCountSerialize(FunctionContext* context, const StringVal& val) {
  if (val.is_null) return val;

  StringVal result = StringVal::CopyFrom(context, val.ptr, val.len);
  context->Free(val.ptr);
  return result;
}

// get distinct count
StringVal DistinctCountFinalize(FunctionContext* context, const StringVal& val) {
  if (val.is_null) return val;
  
  unsigned int count = 0;
  
  uint8_t* str = val.ptr;

  set<string> rs;
  int inx = 0;
  
  for(int i=0;i<val.len;i++){
	if (*str == ','){
		count++;
		if((i-inx) > 0){
			char *item =new char[i-inx];
			memcpy(item,val.ptr+inx,i-inx);
			string s_item(item,0,i-inx);
			if(s_item.length() > 0)
				rs.insert(s_item);
			inx = i+1;
			free(item);
		}
	}

	str++;
  }
  
  if(inx == 0){
	  
	char *item =new char[val.len];
	memcpy(item,val.ptr+inx,val.len);
	string s_item(item,0,val.len);
	if(val.len > 0 && s_item.length() > 0)
		rs.insert(s_item);
	free(item);
  }
  
  if(inx > 0 && (val.len-inx) > 0){
	  
	char *item =new char[val.len-inx];
	memcpy(item,val.ptr+inx,val.len-inx);
	string s_item(item,0,val.len-inx);
	if((val.len-inx)>0 && s_item.length() > 0)
		rs.insert(s_item);
	free(item);
  }

  
  StringVal result;
  
  if (val.len == 0) {
    result = StringVal::null();
  } else {
	  
    result = ToStringVal(context,rs.size());
	
  }
  rs.clear();
  context->Free(val.ptr);
  return result;
}