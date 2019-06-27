aws dynamodb create-table --table-name Forum --attribute-definitions AttributeName=Name,AttributeType=S --key-schema AttributeName=Name,KeyType=HASH --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=1 --profile yiai --region ap-southeast-2

aws dynamodb create-table --table-name Thread --attribute-definitions AttributeName=ForumName,AttributeType=S AttributeName=Subject,AttributeType=S --key-schema AttributeName=ForumName,KeyType=HASH AttributeName=Subject,KeyType=RANGE --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=1 --profile yiai --region ap-southeast-2

aws dynamodb create-table --table-name Reply --attribute-definitions AttributeName=Id,AttributeType=S AttributeName=ReplyDateTime,AttributeType=S --key-schema AttributeName=Id,KeyType=HASH AttributeName=ReplyDateTime,KeyType=RANGE --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=1 --profile yiai --region ap-southeast-2

aws dynamodb batch-write-item --request-items file://forum.json --profile yiai --region ap-southeast-2

aws dynamodb batch-write-item --request-items file://thread.json --profile yiai --region ap-southeast-2

aws dynamodb batch-write-item --request-items file://reply.json --profile yiai --region ap-southeast-2

aws s3api create-bucket --bucket aws-glue-forum.reply.thread.demos --create-bucket-configuration LocationConstraint=ap-southeast-2 --region ap-southeast-2 --profile yiai

CREATE EXTERNAL TABLE IF NOT EXISTS demo.forum (
`threads` bigint,
`category` string,
`messages` bigint,
`views` bigint,
`name` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
'serialization.format' = '1'
) LOCATION 's3://aws-glue-forum.reply.thread.demos/forum/'
TBLPROPERTIES ('has_encrypted_data'='false');

CREATE EXTERNAL TABLE IF NOT EXISTS demo.reply (
`replydatetime` string,
`message` string,
`postedby` string,
`id` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
'serialization.format' = '1'
) LOCATION 's3://aws-glue-forum.reply.thread.demos/reply/'
TBLPROPERTIES ('has_encrypted_data'='false');

CREATE EXTERNAL TABLE IF NOT EXISTS demo.thread (
`views` bigint,
`message` string,
`lastposteddatetime` bigint,
`forumname` string,
`lastpostedby` string,
`replies` bigint,
`answered` bigint,
`tags` array<string>,
`subject` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
'serialization.format' = '1'
) LOCATION 's3://aws-glue-forum.reply.thread.demos/thread/'
TBLPROPERTIES ('has_encrypted_data'='false');

create external table test(
`Tapes` array<struct<
`Status`:string,
`Used`:string,
`Barcode`:string,
`Gateway`:string,
`UsedGB`:string,
`Date`:string,
`SizeGB`:string>>
) ROW FORMAT serde 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://bucket/test'

Presto 0.172 Functions and Operators.

select thread.subject as thread,forum.category , reply.message as reply from thread left join forum on thread.forumname = forum.name left join reply on concat(thread.forumname,'#',thread.subject) like reply.id order by thread;
