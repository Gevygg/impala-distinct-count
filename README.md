# impala-distinct-count
Impala Custom UDAF functions :Support for multi columns distinct count

When you use low version impala to distinct count different columns like "select count(distinct column1),count(distinct column2) from ...", the following error will appear "errorMessage:AnalysisException: all DISTINCT aggregate functions need to have the same set of parameters as ...".So I develop this udaf **distinct_count** to solve this problem.

# Example

For example, there is a Impala table tb1 like this:

column1  | column2  | column3
 ---- | ----- | ------ 
 abc  | ADE | 234
 abc  | CDF | 445  
 abc  | ADE | 237
 abc  | CDF | 445  

You can query with distinct_count() function :
select distinct_count(column1),distinct_count(column2),distinct_count(column3) from tb1
the result is :

1  | 2  | 3
 ---- | ----- | ------
 
# Building
This udaf distinct_count depends on [impala-udf-devel](https://github.com/laserson/impala-udf-devel)  
You should get udf-internal.h,udf.h,udf.cc from impala-udf-devel and compile with sources.  
The target file is libdistinct_count.so  
Put libdistinct_count.so to hdfs path and execute the sql in impala ：  
create aggregate function distinct_count(string) returns string location 'hdfs://xxx/user/impala/udf/libdistinct_count.so' update_fn='DistinctCountUpdate';  
Then you can use this function.

