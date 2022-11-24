#! /bin/bash
echo "------------------ SAMPLE WITH KEY ------------------" 
cat stream1.jsonl | target-oracle --config config.json
echo "------------------ SAMPLE WITHOUT KEY ------------------"
cat stream2.jsonl | target-oracle --config config.json