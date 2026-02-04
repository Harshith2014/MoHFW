# MoHFW BFS Crawler

A robust Breadth-First Search (BFS) web crawler designed to scrape PDF guidelines and notifications from the [Ministry of Health and Family Welfare (MoHFW)](https://mohfw.gov.in) website.

## Features

- **BFS Traversal**: Systematically explores the website using a queue-based approach.
- **Domain Locking**: constrains crawling strictly to `mohfw.gov.in`.
- **Targeted Downloading**: Downloads PDF files larger than 50KB.
- **Metadata Generation**: Creates a generic JSON sidecar for every downloaded PDF.
- **Resilience**: Implements retries, timeouts, and polite delays.

## Prerequisites

- Python 3.x
- Dependencies listed in `Requirements.txt`

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/Harshith2014/MoHFW.git
   cd MoHFW
   ```
2. Install the required packages:

   ```bash
   pip install -r Requirements.txt
   ```
   *Note: Ensure `Requirements.txt` contains valid package names (e.g., `requests`, `beautifulsoup4`).*

## Usage

Run the crawler script:

```bash
python mohfw_bfs_crawler.py
```

## Output

The crawler stores downloaded files and metadata in the following structure:

```
data_dump/
└── General_Medicine/
    └── MoHFW/
        ├── document_name.pdf
        └── document_name.pdf.json
```

## Logging

- Console output shows real-time progress.
- Detailed logs are saved to `crawler.log`.
