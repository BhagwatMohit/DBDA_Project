#!/usr/bin/env python3
# coding: utf-8

import json
import sys

# Read the content of standard input (stdin)
plain_text = sys.stdin.read()

# Convert plain text to JSON object
json_object = json.loads(plain_text)
json_string = json.dumps(json_object)

# Print the JSON object
#print(json_object)
sys.stdout.write(json_string)

