import argparse
from download import main as dl_main
from parse_warc import main as parse_main

def run():
    parser = argparse.ArgumentParser(description="CC-NEWS local prototype")
    parser.add_argument("--download", action="store_true", help="Download WARC files")
    parser.add_argument("--parse", action="store_true", help="Parse WARC files to JSONL")
    args = parser.parse_args()

    if not args.download and not args.parse:
        parser.print_help()
        return

    if args.download:
        dl_main()
    if args.parse:
        parse_main()

if __name__ == "__main__":
    run()
