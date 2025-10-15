# Motherduck Setup Guide

This guide will help you configure and test your Motherduck connection.

## Step 1: Get Your Motherduck Token

1. Log in to https://motherduck.com
2. Go to **Settings** → **API Tokens** (or similar - the UI may vary)
3. Create a new token or copy your existing token
4. The token should look like: `motherduck_xxx...xxx`

## Step 2: Configure Environment Variables

Create a `.env.local` file in the project root (if it doesn't exist):

```bash
# Copy from .env.example
cp .env.example .env.local
```

Edit `.env.local` and add your Motherduck token:

```bash
# Motherduck Connection
MOTHERDUCK_TOKEN=motherduck_your_token_here

# Optional: Database name (defaults to 'kbo')
# Change this if you want to use a different database name
MOTHERDUCK_DATABASE=kbo
```

**Important**:
- `.env.local` is already in `.gitignore` - your token will NOT be committed to git
- Each user can have their own database name by setting `MOTHERDUCK_DATABASE`
- Scripts use dotenv which automatically loads `.env.local` first, then falls back to `.env`

## Step 3: Install DuckDB Node.js Package

We'll use the official DuckDB Node.js client which has built-in Motherduck support:

```bash
npm install duckdb
npm install --save-dev @types/duckdb
```

## Step 4: Test Connection

Run the test script:

```bash
npx tsx scripts/test-motherduck-connection.ts
```

This script will:
1. Connect to Motherduck using your token
2. Create a test database (if needed)
3. Run a simple query
4. Display connection info

## Connection String Format

Motherduck uses a special connection string:

```
md:?motherduck_token=YOUR_TOKEN&database=DATABASE_NAME
```

Or using environment variable:
```
md:DATABASE_NAME
```

With the token set in `MOTHERDUCK_TOKEN` environment variable.

## Troubleshooting

### Error: "Invalid token"
- Check that your token is correctly copied
- Tokens usually start with `motherduck_`
- Make sure there are no extra spaces or newlines

### Error: "Cannot find module 'duckdb'"
- Run: `npm install duckdb`
- Make sure you're in the project root directory

### Error: "Database does not exist"
- The test script will create it automatically
- Or you can create it manually in the Motherduck web UI

### Connection timeout
- Check your internet connection
- Motherduck is a cloud service and requires internet access
- Check if motherduck.com is accessible

## Database Structure

Once connected, your Motherduck database will have:

- **Database name**: `kbo` (configurable via env var)
- **Schema**: All tables from `lib/sql/schema/`
- **Size**: ~100 MB per snapshot, ~2.5 GB for 2 years

## Verifying in Motherduck Web UI

1. Go to https://motherduck.com
2. Navigate to your databases
3. You should see your `kbo` database
4. Click to explore tables (after running schema creation)

## Next Steps

Once connection is verified:

1. **Create schema**: Run `scripts/create-schema.ts`
2. **Initial import**: Run `scripts/initial-import.ts` (when ready)
3. **Test queries**: Use the Motherduck web UI or local scripts

## Security Best Practices

✅ **DO**:
- Store token in `.env.local` (gitignored)
- Use Vercel environment variables for production
- Rotate tokens periodically

❌ **DON'T**:
- Commit tokens to git
- Share tokens in screenshots or logs
- Hardcode tokens in source files

## Connection from Vercel

When deploying to Vercel:

1. Go to your Vercel project settings
2. Environment Variables section
3. Add: `MOTHERDUCK_TOKEN` with your token value
4. Available in all environments or specific to production

## Useful Motherduck Commands

Once connected, you can run:

```sql
-- Show all databases
SHOW DATABASES;

-- Use your database
USE kbo;

-- Show all tables
SHOW TABLES;

-- Get database size
SELECT
  table_name,
  estimated_size
FROM duckdb_tables()
WHERE database_name = 'kbo';

-- Check a table exists
SELECT COUNT(*) FROM information_schema.tables
WHERE table_name = 'enterprises';
```

## Alternative: Using DuckDB CLI

If you have the DuckDB CLI installed:

```bash
# Connect to Motherduck
duckdb md:?motherduck_token=YOUR_TOKEN

# Or with environment variable
export MOTHERDUCK_TOKEN=your_token
duckdb md:kbo
```

## Support

- **Motherduck docs**: https://motherduck.com/docs
- **DuckDB docs**: https://duckdb.org/docs/
- **Node.js client**: https://duckdb.org/docs/api/nodejs/overview
