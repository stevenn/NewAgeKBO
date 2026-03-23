# Architecture Overview

## System Architecture

```mermaid
graph TB
    Admin["👤 Admin User"]

    subgraph Vercel["☁️ Vercel"]
        subgraph NextJS["Next.js App Router"]
            MW["Middleware\n(Route Protection)"]

            subgraph Pages["Admin Pages"]
                PgDash["Dashboard\n(DB Stats & Charts)"]
                PgBrowse["Enterprise Browser\n(Search & Detail)"]
                PgImport["Import Management\n(Jobs & Progress)"]
                PgExport["Export Management\n(VAT Entities)"]
                PgWorkflow["Workflow Monitor"]
            end

            subgraph API["API Routes"]
                APIEnt["/api/enterprises/*\nSearch, Detail, Snapshots"]
                APIImp["/api/admin/imports/*\nStart, Prepare, Process, Finalize"]
                APIExp["/api/admin/exports/*\nCreate, Status, Download"]
                APIMach["/api/machine/*\nExternal Integrations"]
                APIRestate["/api/restate/*\nWebhook Handler"]
            end
        end

        subgraph Libs["Core Libraries"]
            LibAuth["lib/auth\nAdmin & API Key Auth"]
            LibMD["lib/motherduck\nConnection & Queries"]
            LibTemporal["lib/motherduck/temporal\nPoint-in-Time Queries"]
            LibImport["lib/import\nBatch Processing\n& Deduplication"]
            LibExport["lib/export\nVAT Entity Generation"]
            LibKBO["lib/kbo-client\nPortal HTTP Client"]
            LibBlob["lib/blob\nFile Upload/Download"]
            LibRestate["lib/restate\nWorkflow Definitions"]
            LibCache["lib/cache\nCode Descriptions"]
        end
    end

    subgraph External["External Services"]
        Clerk["🔐 Clerk\nAuthentication & Roles"]
        Motherduck["🦆 Motherduck\nHosted DuckDB\n11 tables · 46M+ rows"]
        Restate["🔄 Restate\nDurable Workflow Engine"]
        KBOPortal["🇧🇪 KBO Open Data Portal\nBelgian Government\n(Monthly + Daily Updates)"]
        VBlob["📦 Vercel Blob\nIntermediate File Storage"]
    end

    %% User flow
    Admin -->|HTTPS| MW
    MW -->|Verify Session| Clerk
    MW --> Pages
    Pages --> API

    %% API to libraries
    APIEnt --> LibAuth
    APIEnt --> LibMD
    APIEnt --> LibTemporal
    APIImp --> LibAuth
    APIImp --> LibKBO
    APIImp --> LibBlob
    APIImp --> LibRestate
    APIExp --> LibAuth
    APIExp --> LibExport
    APIMach -->|API Key| LibAuth
    APIMach --> LibExport
    APIRestate --> LibImport

    %% Library to library
    LibExport --> LibMD
    LibImport --> LibMD
    LibImport --> LibBlob
    LibMD --> LibCache

    %% Libraries to external services
    LibAuth --> Clerk
    LibMD --> Motherduck
    LibTemporal --> Motherduck
    LibKBO --> KBOPortal
    LibBlob --> VBlob
    LibRestate --> Restate
    Restate -->|Callback| APIRestate

    %% Styling
    classDef external fill:#e8f4f8,stroke:#0891b2,stroke-width:2px
    classDef page fill:#f0fdf4,stroke:#16a34a,stroke-width:1px
    classDef api fill:#fef3c7,stroke:#d97706,stroke-width:1px
    classDef lib fill:#f5f3ff,stroke:#7c3aed,stroke-width:1px

    class Clerk,Motherduck,Restate,KBOPortal,VBlob external
    class PgDash,PgBrowse,PgImport,PgExport,PgWorkflow page
    class APIEnt,APIImp,APIExp,APIMach,APIRestate api
    class LibAuth,LibMD,LibTemporal,LibImport,LibExport,LibKBO,LibBlob,LibRestate,LibCache lib
```

## Import Data Flow

The import pipeline is the most complex flow, orchestrated by Restate for durability:

```mermaid
sequenceDiagram
    participant Admin
    participant API as Import API
    participant KBO as KBO Portal
    participant Blob as Vercel Blob
    participant RS as Restate
    participant Import as Import Lib
    participant DB as Motherduck

    Admin->>API: POST /imports/start
    API->>KBO: Download ZIP (with session auth)
    KBO-->>API: ZIP file (CSV data)
    API->>Blob: Upload ZIP
    Blob-->>API: Blob URL
    API->>RS: Trigger workflow (blob URL)

    RS->>API: POST /restate (prepare)
    API->>Import: Parse metadata & stage
    Import->>Blob: Download ZIP
    Import->>DB: Create staging tables
    Import-->>RS: Batch count

    loop For each batch
        RS->>API: POST /restate (process-batch)
        API->>Import: Transform & insert
        Import->>DB: INSERT with dedup (ROW_NUMBER)
    end

    RS->>API: POST /restate (finalize)
    API->>Import: Resolve names & cleanup
    Import->>DB: Update primary names
    Import->>Blob: Delete ZIP
```

## Database Schema

```mermaid
erDiagram
    enterprises ||--o{ establishments : "has"
    enterprises ||--o{ denominations : "named as"
    enterprises ||--o{ addresses : "located at"
    enterprises ||--o{ contacts : "contacted via"
    enterprises ||--o{ activities : "classified by"
    enterprises ||--o{ branches : "has foreign"
    activities }o--|| nace_codes : "references"

    enterprises {
        string enterprise_number PK
        date _snapshot_date PK
        int _extract_number PK
        boolean _is_current
        string primary_name
        string status
        string juridical_form
        date start_date
    }

    establishments {
        string establishment_number PK
        string enterprise_number FK
        date start_date
    }

    activities {
        string entity_number PK
        string activity_group PK
        string nace_code FK
        string classification
    }

    denominations {
        string entity_number PK
        int type_of_denomination PK
        string language
        string denomination
    }

    nace_codes {
        string nace_code PK
        string description_nl
        string description_fr
        string description_de
    }

    codes {
        string category PK
        string code PK
        string description_nl
        string description_fr
        string description_de
    }
```

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Motherduck (hosted DuckDB)** | Analytical workload (aggregations over 46M+ rows); columnar storage ideal for KBO data |
| **Restate durable workflows** | Import jobs can take minutes; durability ensures no data loss on Vercel function timeouts |
| **Vercel Blob for intermediary storage** | KBO ZIPs are too large for Restate payloads; blob acts as shared file system |
| **Temporal versioning** | Composite keys (`_snapshot_date`, `_extract_number`) enable point-in-time enterprise queries |
| **Batch processing with dedup** | KBO CSVs contain duplicates; `ROW_NUMBER` windowing ensures last-row-wins semantics |
| **Multi-language via codes table** | KBO provides NL/FR/DE descriptions; joined at query time for user's selected language |
