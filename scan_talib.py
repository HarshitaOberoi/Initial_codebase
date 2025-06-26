import os
import re

def scan_for_talib_patterns(root_dir):
    talib_patterns = [
        'kickingbylength', 'onneck', 'piercing', 'separatinglines', 'ladderbottom',
        'longleggeddoji', 'longline', 'marubozu', 'matchinglow', 'mathold',
        'morningdojistar', 'morningstar', 'rickshawman', 'risefall3methods',
        'shootingstar', 'shortline', 'spinningtop', 'stalledpattern', 'sticksandwich',
        'takuri', 'tasukigap', 'thrusting', 'tristar', 'unique3river', 'upsidegap2crows',
        'xsidegap3methods'
    ]
    pattern_regex = '|'.join(talib_patterns)
    
    for root, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        matches = re.findall(pattern_regex, content, re.IGNORECASE)
                        if matches:
                            print(f"Found TA-Lib patterns in {file_path}: {matches}")
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")

if __name__ == "__main__":
    scan_for_talib_patterns(r"E:\Crypto_with_Real")