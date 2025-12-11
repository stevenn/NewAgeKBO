# DBOS + Supabase Migration Plan

## Goal

Migrate the current manual batch-processing import system to DBOS durable workflows backed by Supabase Postgres, solving reliability issues with long-running tasks.

## Current State

- Manual batch processing via UI clicks ("Process Next Batch", "Auto-Process All")
- State tracked in MotherDuck (`import_job_batches` table)
- No automatic crash recovery - if Vercel times out, must manually resume
- Progress page requires user interaction to drive the import forward

## Target State

- Fully automatic workflow execution via DBOS
- Workflow state tracked in Supabase (DBOS system tables in `dbos` schema)
- Automatic crash recovery and step-level retries
- Vercel cron triggers worker every minute to resume paused workflows
- Simplified progress UI (read-only dashboard with cancel/resume options)

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                      Vercel Next.js App                       │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│   API Routes / Cron Jobs                                      │
│         │                                                     │
│         ▼                                                     │
│   ┌─────────────────────────────────────────────────────┐    │
│   │              DBOS Workflow Engine                    │    │
│   │  • Durable execution       • Auto-retry             │    │
│   │  • Exactly-once semantics  • Crash recovery         │    │
│   └─────────────────────────────────────────────────────┘    │
│         │                              │                      │
│         ▼                              ▼                      │
│   ┌─────────────┐              ┌──────────────┐              │
│   │  Supabase   │              │  MotherDuck  │              │
│   │  (Postgres) │              │  (DuckDB)    │              │
│   │             │              │              │              │
│   │  • Workflow │              │  • KBO data  │              │
│   │    state    │              │  • Analytics │              │
│   │  • dbos.*   │              │  • Exports   │              │
│   │    tables   │              │              │              │
│   └─────────────┘              └──────────────┘              │
└──────────────────────────────────────────────────────────────┘
```

## Cost

- **Supabase Free tier**: $0/month (but projects pause after 7 days inactivity)
- **Supabase Pro**: $25/month (recommended for production, no pausing)
- DBOS workflow state is tiny (<1 MB/month) - won't exceed base storage

## Implementation Phases

### Phase 1: Infrastructure Setup

| Task | Status | Notes |
|------|--------|-------|
| Create Supabase project | TODO | Free/Pro tier at supabase.com/dashboard |
| Get connection string | TODO | Settings → Database → Connection string → URI (Direct connection) |
| Add `DBOS_SYSTEM_DATABASE_URL` to `.env.local` | TODO | Copy from Supabase, replace password |
| Install `@dbos-inc/dbos-sdk` | DONE | Already in package.json |
| Add env var to Vercel | TODO | For production deployment |

### Phase 2: DBOS Core Setup

Create the following files:

#### `lib/dbos/index.ts` - Initialization

```typescript
import { DBOS } from '@dbos-inc/dbos-sdk';

let initialized = false;

export async function initDBOS() {
  if (initialized) return;

  await DBOS.launch();
  initialized = true;
}

export async function shutdownDBOS() {
  if (!initialized) return;

  await DBOS.shutdown();
  initialized = false;
}

export { DBOS };
```

#### `lib/dbos/types.ts` - Progress Types

```typescript
export interface ImportProgress {
  status: 'preparing' | 'processing' | 'finalizing' | 'completed' | 'failed';
  overall: {
    completed: number;
    total: number;
    percentage: number;
  };
  tables: Record<string, {
    completed: number;
    total: number;
    status: 'pending' | 'processing' | 'completed';
  }>;
  currentBatch: {
    table: string;
    batch: number;
    operation: 'delete' | 'insert';
  } | null;
  error?: string;
}
```

#### `lib/dbos/workflows/kbo-import.ts` - Main Workflow

```typescript
import { DBOS } from '@dbos-inc/dbos-sdk';
import { prepareImport, processBatch, finalizeImport } from '@/lib/import/batched-update';
import { downloadFromKbo } from '@/lib/kbo-client';
import type { ImportProgress } from '../types';

// Steps with retry configuration
class KboImportSteps {
  @DBOS.step({ retriesAllowed: true, maxAttempts: 3, intervalSeconds: 5 })
  static async downloadZip(fileUrl: string): Promise<Buffer> {
    DBOS.logger.info(`Downloading from ${fileUrl}`);
    const buffer = await downloadFromKbo(fileUrl);
    return buffer;
  }

  @DBOS.step({ retriesAllowed: true, maxAttempts: 3 })
  static async prepare(zipBuffer: Buffer) {
    DBOS.logger.info('Preparing import...');
    return await prepareImport(zipBuffer, 'vercel');
  }

  @DBOS.step({ retriesAllowed: true, maxAttempts: 5, intervalSeconds: 2 })
  static async processSingleBatch(jobId: string, table: string, batch: number) {
    DBOS.logger.info(`Processing ${table} batch ${batch}`);
    return await processBatch(jobId, table, batch);
  }

  @DBOS.step({ retriesAllowed: true, maxAttempts: 3 })
  static async finalize(jobId: string) {
    DBOS.logger.info(`Finalizing job ${jobId}`);
    return await finalizeImport(jobId);
  }
}

// Main durable workflow
class KboImportWorkflow {
  @DBOS.workflow()
  static async importDailyUpdate(fileUrl: string) {
    // Initialize progress event
    await DBOS.setEvent<ImportProgress>('progress', {
      status: 'preparing',
      overall: { completed: 0, total: 0, percentage: 0 },
      tables: {},
      currentBatch: null,
    });

    // Step 1: Download ZIP
    const zipBuffer = await KboImportSteps.downloadZip(fileUrl);

    // Step 2: Prepare (parse ZIP, create batches in MotherDuck)
    const { job_id, batches_by_table, total_batches } =
      await KboImportSteps.prepare(zipBuffer);

    // Initialize table progress tracking
    const tableProgress: Record<string, { completed: number; total: number; status: string }> = {};
    for (const [table, counts] of Object.entries(batches_by_table)) {
      tableProgress[table] = {
        completed: 0,
        total: counts.delete + counts.insert,
        status: 'pending',
      };
    }

    let completedBatches = 0;

    // Step 3: Process all batches (each is a durable checkpoint)
    for (const [table, counts] of Object.entries(batches_by_table)) {
      tableProgress[table].status = 'processing';
      const tableBatches = counts.delete + counts.insert;

      for (let i = 1; i <= tableBatches; i++) {
        // Update progress before processing
        await DBOS.setEvent<ImportProgress>('progress', {
          status: 'processing',
          overall: {
            completed: completedBatches,
            total: total_batches,
            percentage: Math.round((completedBatches / total_batches) * 100),
          },
          tables: { ...tableProgress },
          currentBatch: {
            table,
            batch: i,
            operation: i <= counts.delete ? 'delete' : 'insert'
          },
        });

        // Process batch - if timeout here, DBOS resumes from this exact point
        await KboImportSteps.processSingleBatch(job_id, table, i);

        completedBatches++;
        tableProgress[table].completed++;
      }

      tableProgress[table].status = 'completed';
    }

    // Step 4: Finalize
    await DBOS.setEvent<ImportProgress>('progress', {
      status: 'finalizing',
      overall: { completed: total_batches, total: total_batches, percentage: 100 },
      tables: tableProgress,
      currentBatch: null,
    });

    const result = await KboImportSteps.finalize(job_id);

    // Final progress update
    await DBOS.setEvent<ImportProgress>('progress', {
      status: 'completed',
      overall: { completed: total_batches, total: total_batches, percentage: 100 },
      tables: tableProgress,
      currentBatch: null,
    });

    return { job_id, names_resolved: result.names_resolved };
  }
}

export const kboImportWorkflow = KboImportWorkflow.importDailyUpdate;
```

### Phase 3: API Routes

#### `app/api/dbos/worker/route.ts` - Cron Worker

```typescript
import { DBOS } from '@dbos-inc/dbos-sdk';
import { initDBOS } from '@/lib/dbos';

export const maxDuration = 300; // 5 minutes (Vercel Pro limit)
export const dynamic = 'force-dynamic';

export async function GET() {
  await initDBOS();

  // Poll for pending workflows and execute them
  // If timeout occurs, next cron invocation continues
  await DBOS.executeWorkflowWorker();

  return new Response('OK', { status: 200 });
}
```

#### `app/api/dbos/imports/route.ts` - Start & List Workflows

```typescript
import { DBOS } from '@dbos-inc/dbos-sdk';
import { initDBOS } from '@/lib/dbos';
import { kboImportWorkflow } from '@/lib/dbos/workflows/kbo-import';
import { NextResponse } from 'next/server';

export async function POST(request: Request) {
  await initDBOS();

  const { fileUrl } = await request.json();

  // Start workflow - returns immediately with workflow ID
  const handle = await DBOS.startWorkflow(kboImportWorkflow, fileUrl);

  return NextResponse.json({
    workflow_id: handle.workflowID,
    status: 'started',
  });
}

export async function GET() {
  await initDBOS();

  // List recent workflows
  const workflows = await DBOS.listWorkflows({
    limit: 50,
  });

  return NextResponse.json({ workflows });
}
```

#### `app/api/dbos/imports/[workflowId]/progress/route.ts` - Get Progress

```typescript
import { DBOS } from '@dbos-inc/dbos-sdk';
import { initDBOS } from '@/lib/dbos';
import type { ImportProgress } from '@/lib/dbos/types';
import { NextResponse } from 'next/server';

export async function GET(
  request: Request,
  { params }: { params: Promise<{ workflowId: string }> }
) {
  await initDBOS();

  const { workflowId } = await params;

  // Get workflow status
  const status = await DBOS.getWorkflowStatus(workflowId);

  // Get progress event (1s timeout - returns null if not set yet)
  const progress = await DBOS.getEvent<ImportProgress>(workflowId, 'progress', 1);

  return NextResponse.json({
    workflow_id: workflowId,
    workflow_status: status?.status,
    ...progress,
  });
}
```

#### `app/api/dbos/imports/[workflowId]/cancel/route.ts` - Cancel

```typescript
import { DBOS } from '@dbos-inc/dbos-sdk';
import { initDBOS } from '@/lib/dbos';
import { NextResponse } from 'next/server';

export async function POST(
  request: Request,
  { params }: { params: Promise<{ workflowId: string }> }
) {
  await initDBOS();

  const { workflowId } = await params;
  await DBOS.cancelWorkflow(workflowId);

  return NextResponse.json({ cancelled: true });
}
```

#### `app/api/dbos/imports/[workflowId]/resume/route.ts` - Resume

```typescript
import { DBOS } from '@dbos-inc/dbos-sdk';
import { initDBOS } from '@/lib/dbos';
import { NextResponse } from 'next/server';

export async function POST(
  request: Request,
  { params }: { params: Promise<{ workflowId: string }> }
) {
  await initDBOS();

  const { workflowId } = await params;
  await DBOS.resumeWorkflow(workflowId);

  return NextResponse.json({ resumed: true });
}
```

### Phase 4: Vercel Cron Configuration

Create `vercel.json`:

```json
{
  "crons": [
    {
      "path": "/api/dbos/worker",
      "schedule": "* * * * *"
    }
  ]
}
```

### Phase 5: Update Progress UI

Modify `app/admin/imports/[jobId]/progress/page.tsx`:

**Remove:**
- "Process Next Batch" button
- "Auto-Process All" toggle
- "Finalize" button

**Add:**
- "Cancel Import" button (calls `/api/dbos/imports/[workflowId]/cancel`)
- "Resume Import" button (for failed/cancelled workflows)

**Update:**
- Change API endpoint from `/api/admin/imports/[jobId]/progress` to `/api/dbos/imports/[workflowId]/progress`
- Simplify state management (no more processing/autoProcess states)

### Phase 6: Migration & Cleanup (Optional, Later)

| Task | Description |
|------|-------------|
| Add feature flag | `USE_DBOS_WORKFLOWS=true` in env |
| Keep old routes | For rollback capability |
| Remove old batch tables | After validation period |
| Simplify batched-update.ts | Remove manual state management |

## Files Summary

### New Files (9)

```
lib/dbos/index.ts
lib/dbos/types.ts
lib/dbos/workflows/kbo-import.ts
app/api/dbos/worker/route.ts
app/api/dbos/imports/route.ts
app/api/dbos/imports/[workflowId]/progress/route.ts
app/api/dbos/imports/[workflowId]/cancel/route.ts
app/api/dbos/imports/[workflowId]/resume/route.ts
vercel.json
```

### Modified Files (2)

```
.env.example                                    # DONE - Added DBOS_SYSTEM_DATABASE_URL
app/admin/imports/[jobId]/progress/page.tsx     # Simplify to read-only dashboard
```

## Testing Checklist

- [ ] Supabase connection works locally
- [ ] Workflow starts and progress events emit
- [ ] Progress UI updates via polling
- [ ] Simulate timeout: kill process mid-workflow, verify resume on next worker run
- [ ] Test step retries: mock network failure in download step
- [ ] Cancel workflow works
- [ ] Resume cancelled/failed workflow works
- [ ] Verify no duplicate processing (exactly-once semantics)

## Rollback Plan

1. Set feature flag `USE_DBOS_WORKFLOWS=false`
2. Old import system at `/api/admin/imports/*` remains functional
3. No KBO data loss - all data still in MotherDuck

## References

- [DBOS + Supabase Integration](https://docs.dbos.dev/integrations/supabase)
- [DBOS Workflows & Steps](https://docs.dbos.dev/typescript/reference/workflows-steps)
- [DBOS Workflow Communication (Events)](https://docs.dbos.dev/typescript/tutorials/workflow-communication-tutorial)
- [DBOS Vercel Integration](https://docs.dbos.dev/integrations/vercel)
- [Supabase Pricing](https://supabase.com/pricing)
