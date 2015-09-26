#!/bin/bash
RULE_FILE_NAME='../ios_rules/ios_usa_agent.rule'
RULE_FILE_NAME_BIN="$RULE_FILE_NAME".bin
FILES=~/pcaps/ios/usa/mingxiao1998-outlook/20150812/*
./ipsc $RULE_FILE_NAME $RULE_FILE_NAME_BIN
for f in ~/pcaps/ios/usa/mingxiao1998-outlook/20150812/396659191.pcap
do 
  echo 'Processing', $f
  ./iscan -akpvr $RULE_FILE_NAME_BIN $f
done
