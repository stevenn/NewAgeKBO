# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This repository is being developed to work with **KBO (Crossroads Bank for Enterprises) Open Data** from the Belgian Federal Public Service Economy. The KBO database contains information about all Belgian enterprises and establishments.

The project is currently in its initial phase with only specification documentation available. Future development will focus on processing and analyzing KBO Open Data files.

## KBO Open Data Structure

The KBO Open Data is provided as monthly full files and daily update files, available via https://kbopub.economie.fgov.be/kbo-open-data (registration required).

### File Format
- **Format**: ZIP files containing CSV files
- **CSV Characteristics**:
  - Delimiter: comma (`,`)
  - Text delimiter: double quotes (`"`)
  - Decimal point: period (`.`)
  - Date format: `dd-mm-yyyy`
  - Empty values: consecutive delimiters (no space)

### Data Files
The ZIP archives contain these CSV files:

1. **meta.csv**: Metadata (snapshot date, extract timestamp, version, extract number)
2. **code.csv**: Code descriptions used in other files (multi-language: NL, FR, DE, EN)
3. **enterprise.csv**: One line per enterprise with enterprise number, status, juridical form, start date
4. **establishment.csv**: One line per establishment with establishment number, start date, parent enterprise number
5. **denomination.csv**: Names (legal, commercial, abbreviations) for enterprises/establishments
6. **address.csv**: Addresses (legal persons: seat + optional branch; natural persons: establishment addresses only)
7. **contact.csv**: Contact details (phone, email, web) for enterprises/establishments
8. **activity.csv**: Economic activities using NACE codes (2003, 2008, or 2025 versions)
9. **branch.csv**: Branch offices of foreign entities in Belgium

### Key Identifiers
- **Enterprise Number**: Format `9999.999.999` (10 digits with dots)
- **Establishment Number**: Format `9.999.999.999` (10 digits with leading single digit)

### Update File Logic
Update files use a delete-then-insert pattern:
1. Files ending in `_delete.csv` contain entity/establishment numbers to remove
2. Files ending in `_insert.csv` contain complete replacement data (not just changes)
3. Process in order: delete first, then insert

Example: If an enterprise's name changes, `denomination_delete.csv` contains the enterprise number, and `denomination_insert.csv` contains ALL current names for that enterprise (not just the changed one).

## Data Characteristics

- **Scope**: Only active enterprises and active establishments
- **History**: No historical data - only current state as of snapshot date
- **Languages**: Multi-language support (Dutch, French, German, English) for codes and addresses
- **Updates**: Full file monthly (first Sunday), update files daily

## Reference Documentation

Complete technical specifications are available in:
- `/specs/KBOCookbook_EN.md` - English version (R018.00)
- `/specs/KBOCookbook_NL.md` - Dutch version (R018.00)

These specifications were extracted and translated from the official PDF: https://economie.fgov.be/sites/default/files/Files/Entreprises/KBO/Cookbook-KBO-Open-Data.pdf

## Future Development

When building tools for this project, consider:
- CSV parsing with proper handling of quoted fields and null values
- Multi-language support for code descriptions
- Efficient processing of large datasets (full database contains all Belgian enterprises)
- Relational data modeling (enterprises → establishments → addresses/activities/contacts)
- Update file processing with delete-then-insert pattern

## Project Vision: KBO for the New Age

- The goal of this project is to provide a more modern experience for people who want to build an application on top of the KBO Open Data set.
- The target deployment for this project is a Vercel Next.js webapp running against a Motherduck hosted database (https://motherduck.com/docs/sql-reference/).
- The admin application automatically fetches KBO updates from https://kbopub.economie.fgov.be/kbo-open-data/affiliation/xml/?files (username + password auth), and maintains a live database which tracks the data updates in these files.
- The database schema must allow for time-based navigation, making it possible to build analytical queries comparing time periods. Timestamps are maintained on a day-based granularity and can be found in meta.csv for data updates.
- The application takes specific care for supporting the codetables defined in code.csv and the multi-lingual character of the dataset.
- The central entity of the dataset is the enterprise, which has an optional subhierarchy of establishments.

## Considerations

- Always check size of sample CSV files prior to reading them, some files are very large!
- The sampledata directory contains a local copy of a monthly (full) dataset, and a daily increment.

### Admin webapp
- Allows to list & fetch datasets from the KBO Open Data service and saves them in a landing zone on Motherduck
- Triggers data manipulation actions, preferably running in Motherduck as SQL jobs (i.e. treat as little data locally as possible)
- UX is decoupled from the backend: the backend operations should also be programmatically accessible via https://vercel.com/docs/cron-jobs
- Allows to look at import jobs
- Allows to browse the resulting database