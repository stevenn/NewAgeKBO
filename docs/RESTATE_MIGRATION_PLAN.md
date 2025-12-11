# Restate Migration Plan

## Goal

Migrate the current manual batch-processing import system to Restate durable workflows, solving reliability issues with long-running tasks.

## What is Restate?

Restate is an open-source durable execution engine that provides:
- **Durable async/await**: Every `ctx.run()` call is checkpointed
- **Automatic retries**: Failed steps retry with backoff
- **Exactly-once execution**: Steps never re-execute after completion
- **Scale-to-zero**: Serverless-friendly, handlers can suspend/resume

## Architecture Comparison: DBOS vs Restate

| Aspect | DBOS | Restate |
|--------|------|---------|
| **Architecture** | Embedded library | External server + SDK |
| **State Storage** | Postgres (e.g., Supabase) | Restate Server (Raft log) |
| **Infrastructure** | Just add Postgres | Need Restate Server OR Restate Cloud |
| **How it works** | Cron polls for paused workflows | Server pushes events to handlers |
| **Vercel integration** | Function calls DBOS library | Restate Server calls Vercel functions |
| **Code changes** | Decorators on existing functions | Wrap handlers in Restate service |

## Restate Architecture for NewAgeKBO

```
┌─────────────────────────────────────────────────────────────────────┐
│                         External Request                             │
│                    (Start import workflow)                           │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Restate Server/Cloud                            │
│                                                                      │
│  • Receives invocation requests                                      │
│  • Persists workflow state (Raft log)                               │
│  • Pushes events to handlers                                         │
│  • Tracks step completion                                            │
│  • Handles retries and recovery                                      │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                    Invokes handlers via HTTP
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Vercel Functions                                │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │              Restate Handler (KBO Import)                    │   │
│  │                                                              │   │
│  │  ctx.run("download", () => downloadZip(url))                │   │
│  │  ctx.run("prepare", () => prepareImport(buffer))            │   │
│  │  for each batch:                                             │   │
│  │    ctx.run("batch-N", () => processBatch(...))              │   │
│  │  ctx.run("finalize", () => finalizeImport(jobId))           │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              │                                       │
│                              ▼                                       │
│                      ┌──────────────┐                               │
│                      │  MotherDuck  │                               │
│                      │  (KBO data)  │                               │
│                      └──────────────┘                               │
└─────────────────────────────────────────────────────────────────────┘
```

## Hosting Options for Restate Server

### Option A: Restate Cloud (Recommended for simplicity)

| Aspect | Details |
|--------|---------|
| **Free tier** | Yes, no credit card required |
| **Limits** | ~30-40 events/second (sufficient for imports) |
| **SLA** | No strict SLA on free tier |
| **Setup** | Create account, get environment URL |
| **Best for** | Development, testing, non-mission-critical |

### Option B: Self-Hosted on Fly.io/Railway

| Aspect | Details |
|--------|---------|
| **Cost** | ~$5-10/month for small instance |
| **Control** | Full control over server |
| **Setup** | Deploy Docker container |
| **Best for** | Production with cost control |

### Option C: Self-Hosted on AWS/GCP

| Aspect | Details |
|--------|---------|
| **Cost** | Variable based on instance size |
| **Control** | Full control |
| **Setup** | More complex, need networking |
| **Best for** | Enterprise, existing cloud infra |

## Cost Comparison

| Solution | Monthly Cost | Notes |
|----------|-------------|-------|
| **DBOS + Supabase Free** | $0 | Projects pause after 7 days |
| **DBOS + Supabase Pro** | $25 | Recommended for production |
| **Restate Cloud Free** | $0 | Limited throughput, no SLA |
| **Restate Cloud Paid** | TBD (contact sales) | For production SLA |
| **Restate Self-Hosted** | $5-15 | Fly.io/Railway/small VM |

## Implementation Phases

### Phase 1: Infrastructure Setup

| Task | Status | Notes |
|------|--------|-------|
| Choose Restate hosting option | TODO | Cloud free tier for dev, self-host for prod |
| Create Restate Cloud account | TODO | Or deploy self-hosted |
| Get Restate ingress URL | TODO | e.g., `https://your-env.restate.cloud` |
| Install `@restatedev/restate-sdk` | TODO | npm package |
| Add env vars | TODO | `RESTATE_INGRESS_URL`, `RESTATE_ADMIN_URL` |

### Phase 2: Create Restate Handlers

#### Install SDK

```bash
npm install @restatedev/restate-sdk
```

#### `lib/restate/kbo-import-service.ts` - Import Service

```typescript
import * as restate from "@restatedev/restate-sdk";
import { prepareImport, processBatch, finalizeImport } from "@/lib/import/batched-update";
import { downloadFromKbo } from "@/lib/kbo-client";

// Define the import workflow as a Restate Workflow
const kboImportWorkflow = restate.workflow({
  name: "KboImport",
  handlers: {
    // Main workflow handler - runs exactly once per workflow ID
    run: async (ctx: restate.WorkflowContext, fileUrl: string) => {
      // Step 1: Download ZIP (durable, retried on failure)
      const zipBuffer = await ctx.run("download-zip", async () => {
        return await downloadFromKbo(fileUrl);
      });

      // Step 2: Prepare import
      const { job_id, batches_by_table, total_batches } = await ctx.run(
        "prepare-import",
        async () => {
          return await prepareImport(zipBuffer, "vercel");
        }
      );

      // Update progress state
      ctx.set("progress", {
        status: "processing",
        completed: 0,
        total: total_batches,
        job_id,
      });

      // Step 3: Process each batch (each is a durable checkpoint)
      let completedBatches = 0;
      for (const [table, counts] of Object.entries(batches_by_table)) {
        const tableBatches = counts.delete + counts.insert;

        for (let i = 1; i <= tableBatches; i++) {
          await ctx.run(`process-${table}-${i}`, async () => {
            await processBatch(job_id, table, i);
          });

          completedBatches++;
          ctx.set("progress", {
            status: "processing",
            completed: completedBatches,
            total: total_batches,
            currentTable: table,
            currentBatch: i,
            job_id,
          });
        }
      }

      // Step 4: Finalize
      ctx.set("progress", {
        status: "finalizing",
        completed: total_batches,
        total: total_batches,
        job_id,
      });

      const result = await ctx.run("finalize", async () => {
        return await finalizeImport(job_id);
      });

      ctx.set("progress", {
        status: "completed",
        completed: total_batches,
        total: total_batches,
        job_id,
        names_resolved: result.names_resolved,
      });

      return { job_id, names_resolved: result.names_resolved };
    },

    // Shared handler to get progress (can be called while workflow runs)
    getProgress: restate.handlers.workflow.shared(
      async (ctx: restate.WorkflowSharedContext) => {
        return ctx.get("progress") || { status: "pending" };
      }
    ),
  },
});

export default kboImportWorkflow;
```

#### `app/api/restate/route.ts` - Restate HTTP Endpoint

```typescript
import * as restate from "@restatedev/restate-sdk/fetch";
import kboImportWorkflow from "@/lib/restate/kbo-import-service";

// Create Restate endpoint that Restate Server will call
const handler = restate
  .endpoint()
  .bind(kboImportWorkflow)
  .handler();

export const POST = handler.fetch;
```

### Phase 3: API Routes for Client

#### `app/api/imports/start/route.ts` - Start Import

```typescript
import { NextResponse } from "next/server";

const RESTATE_INGRESS_URL = process.env.RESTATE_INGRESS_URL!;

export async function POST(request: Request) {
  const { fileUrl, workflowId } = await request.json();

  // Start workflow via Restate ingress
  const response = await fetch(
    `${RESTATE_INGRESS_URL}/KboImport/${workflowId}/run/send`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(fileUrl),
    }
  );

  if (!response.ok) {
    return NextResponse.json(
      { error: "Failed to start workflow" },
      { status: 500 }
    );
  }

  return NextResponse.json({
    workflow_id: workflowId,
    status: "started",
  });
}
```

#### `app/api/imports/[workflowId]/progress/route.ts` - Get Progress

```typescript
import { NextResponse } from "next/server";

const RESTATE_INGRESS_URL = process.env.RESTATE_INGRESS_URL!;

export async function GET(
  request: Request,
  { params }: { params: Promise<{ workflowId: string }> }
) {
  const { workflowId } = await params;

  // Call shared handler to get progress
  const response = await fetch(
    `${RESTATE_INGRESS_URL}/KboImport/${workflowId}/getProgress`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "null",
    }
  );

  if (!response.ok) {
    return NextResponse.json(
      { error: "Failed to get progress" },
      { status: 500 }
    );
  }

  const progress = await response.json();
  return NextResponse.json(progress);
}
```

### Phase 4: Register Service with Restate

After deploying to Vercel, register your handler with Restate:

```bash
# Using Restate CLI
restate deployments register https://your-app.vercel.app/api/restate

# Or via Admin API
curl -X POST ${RESTATE_ADMIN_URL}/deployments \
  -H "Content-Type: application/json" \
  -d '{"uri": "https://your-app.vercel.app/api/restate"}'
```

**Note**: For Vercel preview deployments, each gets a unique URL. Restate handles this well - old versions scale to zero and only wake when needed.

### Phase 5: Update Progress UI

Similar to DBOS plan - remove manual processing buttons, make it read-only:

**Remove:**
- "Process Next Batch" button
- "Auto-Process All" toggle
- "Finalize" button

**Add:**
- "Cancel Import" button (via Restate Admin API)

**Update:**
- Change API endpoint to `/api/imports/[workflowId]/progress`

## Environment Variables

```bash
# .env.local

# Restate Cloud (or self-hosted server URL)
RESTATE_INGRESS_URL=https://your-env.restate.cloud:8080
RESTATE_ADMIN_URL=https://your-env.restate.cloud:9070

# Existing
MOTHERDUCK_TOKEN=...
MOTHERDUCK_DATABASE=kbo
```

## Files Summary

### New Files (5)

```
lib/restate/kbo-import-service.ts    # Workflow definition
app/api/restate/route.ts              # Restate handler endpoint
app/api/imports/start/route.ts        # Start workflow API
app/api/imports/[workflowId]/progress/route.ts  # Progress API
```

### Modified Files (2)

```
.env.example                          # Add RESTATE_* vars
app/admin/imports/[jobId]/progress/page.tsx  # Simplify UI
```

## Key Differences from DBOS Approach

| Aspect | DBOS | Restate |
|--------|------|---------|
| **Server** | None (library only) | Restate Server required |
| **State location** | Your Postgres | Restate's internal log |
| **Invocation model** | Your code calls DBOS | Restate calls your handlers |
| **Timeout handling** | Cron polls paused workflows | Server suspends/resumes handlers |
| **Vercel integration** | Worker cron every minute | Restate pushes to handler URL |
| **Debugging** | Query Postgres tables | Restate UI + CLI |

## Pros and Cons

### Restate Pros
- More mature workflow model (similar to Temporal)
- Better observability (Restate UI, CLI tools)
- No Postgres dependency
- Works well with serverless versioned URLs
- Supports more complex patterns (signals, timers, state machines)

### Restate Cons
- Need to run/pay for Restate Server (or use Cloud)
- More moving parts (server + handlers)
- Learning curve for Restate concepts
- Less "invisible" than DBOS decorators

### DBOS Pros
- Just a library - no external server
- Uses familiar Postgres (Supabase)
- Simpler mental model
- Lower infrastructure overhead

### DBOS Cons
- Requires Postgres
- Less mature than Restate
- Fewer workflow patterns supported

## Recommendation

**For NewAgeKBO specifically:**

| If you prefer... | Choose |
|------------------|--------|
| Simplicity, less infrastructure | **DBOS + Supabase** |
| Better observability, proven patterns | **Restate** |
| No external dependencies | **DBOS** |
| Free tier without limitations | **Restate Cloud** (no project pausing) |

Both are valid choices. DBOS is simpler (just add Postgres), while Restate is more feature-rich (but needs a server).

## Testing Checklist

- [ ] Restate Server/Cloud accessible
- [ ] Handler registered successfully (`restate deployments list`)
- [ ] Workflow starts via API
- [ ] Progress updates visible in UI
- [ ] Simulate crash: stop handler mid-workflow, verify resume
- [ ] Verify exactly-once execution (no duplicate batches)
- [ ] Test Vercel deployment with Restate Cloud

## References

- [Restate Documentation](https://docs.restate.dev/)
- [Restate TypeScript SDK](https://docs.restate.dev/develop/ts/overview/)
- [Restate Workflows](https://docs.restate.dev/develop/ts/workflows/)
- [Restate Cloud](https://www.restate.dev/cloud)
- [Restate GitHub](https://github.com/restatedev/restate)
- [AWS Lambda Deployment](https://docs.restate.dev/services/deploy/lambda)
