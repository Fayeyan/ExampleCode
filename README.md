# ExampleCode
This is Example Code for a game recommendation engine.

usage: run_engine.py [-h] [--config CONFIG] [--output_path OUTPUT_PATH]
                     [--output_format {json,xml,html}]
                     input_file

Game Recommendation Engine

positional arguments:

  input_file                                          Path to user id file

optional arguments:

  -h, --help                                          show this help message and exit
  
  --config CONFIG, -c CONFIG                          Path to config file
  
  --output_path OUTPUT_PATH, -o OUTPUT_PATH           Path to output file folder

  --output_format {json,xml,html}, -f {json,xml,html} Format of the output file, currently supported: JSON
