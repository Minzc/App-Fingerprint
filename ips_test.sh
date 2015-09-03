RULE_FILE_NAME='kv.rule.head'
RULE_FILE_NAME_BIN = "$RULE_FILE_NAME".bin
FILES='~/pcaps/ios/usa/mingxiao1998-outlook/20150812/'
./ipsc $RULE_FILE_NAME $RULE_FILE_NAME_BIN
for f in $FILES:
do 
  echo 'Processing', $f
done
