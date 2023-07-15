@echo off
echo ------------------ SAMPLE WITH KEY ------------------
type stream1.jsonl | target-mysql --config config.json
echo ------------------ SAMPLE WITH KEY ------------------
type stream2.jsonl | target-mysql --config config.json
echo ------------------ SAMPLE WITH KEY ------------------
type stream3.jsonl | target-mysql --config config.json