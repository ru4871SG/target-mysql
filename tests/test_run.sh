#! /bin/bash
echo "------------------ SAMPLE WITH KEY ------------------" 
cat stream1.jsonl | target-mysql --config config.json
echo "------------------ SAMPLE WITHOUT KEY ------------------"
cat stream2.jsonl | target-mysql --config config.json
echo "------------------ The Horvath ------------------"
cat stream3.jsonl | target-mysql --config config.json