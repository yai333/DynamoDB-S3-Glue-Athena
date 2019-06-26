aws dynamodb create-table --table-name Forum --attribute-definitions AttributeName=Name,AttributeType=S --key-schema AttributeName=Name,KeyType=HASH --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=1 --profile yiai --region ap-southeast-2


aws dynamodb create-table --table-name Thread --attribute-definitions AttributeName=ForumName,AttributeType=S AttributeName=Subject,AttributeType=S --key-schema AttributeName=ForumName,KeyType=HASH AttributeName=Subject,KeyType=RANGE --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=1 --profile yiai --region ap-southeast-2


aws dynamodb create-table --table-name Reply --attribute-definitions AttributeName=Id,AttributeType=S AttributeName=ReplyDateTime,AttributeType=S --key-schema AttributeName=Id,KeyType=HASH AttributeName=ReplyDateTime,KeyType=RANGE --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=1 --profile yiai --region ap-southeast-2


 
aws dynamodb batch-write-item --request-items file://forum.json --profile yiai --region ap-southeast-2

aws dynamodb batch-write-item --request-items file://thread.json --profile yiai --region ap-southeast-2

aws dynamodb batch-write-item --request-items file://reply.json --profile yiai --region ap-southeast-2

aws s3api create-bucket --bucket aws-glue-forum.reply.thread.demos --create-bucket-configuration LocationConstraint=ap-southeast-2 --region ap-southeast-2 --profile yiai
