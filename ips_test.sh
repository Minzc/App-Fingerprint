RULE_FILE_NAME='kv.rules.head'
FILES=''
./ips $RULE_FILE_NAME $RULE_FILE_NAME.bin
for f in $FILES:
do 
  echo 'Processing', $f
done
