#!/usr/bin/env python3
"""
Script to deduplicate paths in a file.
Removes duplicate lines while preserving the order of first occurrence.
"""

import argparse
import sys
from pathlib import Path


def deduplicate_file(input_file, output_file=None, preserve_order=True):
    """
    Deduplicate paths in a file.
    
    Args:
        input_file (str): Path to input file
        output_file (str, optional): Path to output file. If None, overwrites input file.
        preserve_order (bool): Whether to preserve order of first occurrence
    
    Returns:
        tuple: (original_count, deduplicated_count)
    """
    try:
        # Read all lines from input file
        with open(input_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        original_count = len(lines)
        
        if preserve_order:
            # Use dict to preserve order (Python 3.7+) while removing duplicates
            seen = {}
            unique_lines = []
            for line in lines:
                stripped_line = line.strip()
                if stripped_line and stripped_line not in seen:
                    seen[stripped_line] = True
                    unique_lines.append(line)
        else:
            # Use set for faster deduplication (doesn't preserve order)
            unique_lines = list(set(line.strip() for line in lines if line.strip()))
            unique_lines = [line + '\n' for line in unique_lines]
        
        deduplicated_count = len(unique_lines)
        
        # Determine output file
        out_file = output_file if output_file else input_file
        
        # Write deduplicated lines to output file
        with open(out_file, 'w', encoding='utf-8') as f:
            f.writelines(unique_lines)
        
        return original_count, deduplicated_count
        
    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found.", file=sys.stderr)
        sys.exit(1)
    except PermissionError:
        print(f"Error: Permission denied accessing files.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Deduplicate paths in a file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python deduplicate_paths.py input.txt
  python deduplicate_paths.py input.txt -o output.txt
  python deduplicate_paths.py input.txt --no-preserve-order
        """
    )
    
    parser.add_argument(
        'input_file',
        help='Input file containing paths to deduplicate'
    )
    
    parser.add_argument(
        '-o', '--output',
        dest='output_file',
        help='Output file (default: overwrite input file)'
    )
    
    parser.add_argument(
        '--no-preserve-order',
        action='store_false',
        dest='preserve_order',
        help='Do not preserve order of first occurrence (faster for large files)'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Show detailed output'
    )
    
    args = parser.parse_args()
    
    # Check if input file exists
    if not Path(args.input_file).exists():
        print(f"Error: Input file '{args.input_file}' does not exist.", file=sys.stderr)
        sys.exit(1)
    
    # Perform deduplication
    original_count, deduplicated_count = deduplicate_file(
        args.input_file,
        args.output_file,
        args.preserve_order
    )
    
    # Show results
    duplicates_removed = original_count - deduplicated_count
    output_file = args.output_file if args.output_file else args.input_file
    
    if args.verbose:
        print(f"Input file: {args.input_file}")
        print(f"Output file: {output_file}")
        print(f"Original lines: {original_count}")
        print(f"Unique lines: {deduplicated_count}")
        print(f"Duplicates removed: {duplicates_removed}")
    else:
        print(f"Removed {duplicates_removed} duplicate(s) from {original_count} lines")
        if args.output_file:
            print(f"Results saved to: {output_file}")
        else:
            print(f"File updated in place: {output_file}")


if __name__ == "__main__":
    main()