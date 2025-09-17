# ETL Pipeline for Cloud Databases
2025-09-16

Holland Reece Brown (https://github.com/holland-reece)

### Description
- Extracts, transforms, and loads online data about world banks
- Creates and updates a log file
- Saves final organized table to a local CSV and a cloud-based database
- Performs SQL queries via database
- Written for final project in IBM Engineering Foundations program

### Dependencies
- Python 3.13
    - os
    - requests
    - BeautifulSoup
    - numpy
    - pandas
    - sqlite3
    - datetime
    - typing
    - csv
- Cloud database service configured so you can connect with SQLite
    - Script creates a new databse with connection given to SQLite API
- Read/write permissions on local machine
    - Script saves data as a CSV in your working (or specified) dir
    - Also creates and edits text files (log)

### Dataset
- Data loaded from World Banks table in Wikipedia archive
- URL: </https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks>
