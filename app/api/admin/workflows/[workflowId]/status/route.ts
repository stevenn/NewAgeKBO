/**
 * Get KBO Import Workflow Status
 *
 * GET /api/admin/imports/[workflowId]/status
 *
 * Fetches progress from Restate for a running or completed import workflow.
 * During "preparing" phase, also queries database for staging progress.
 */

import { NextResponse } from "next/server";
import { connectMotherduck, closeMotherduck, executeQuery } from "@/lib/motherduck";
import { createHash } from "crypto";

const RESTATE_INGRESS_URL = process.env.RESTATE_INGRESS_URL || "http://localhost:8080";
const RESTATE_AUTH_TOKEN = process.env.RESTATE_AUTH_TOKEN;

/**
 * Generate deterministic job ID from workflow ID (must match batched-update.ts)
 */
function generateJobId(workflowId: string): string {
  const hash = createHash('sha256').update(workflowId).digest('hex');
  return `${hash.slice(0, 8)}-${hash.slice(8, 12)}-${hash.slice(12, 16)}-${hash.slice(16, 20)}-${hash.slice(20, 32)}`;
}

/**
 * Query database for preparation progress
 * @param jobIdOrWorkflowId - Either a job ID (from Restate progress) or workflow ID to generate from
 */
async function getPreparationProgress(jobIdOrWorkflowId: string): Promise<{
  job_status?: string;
  staging_counts?: Record<string, number>;
  extract_number?: number;
  snapshot_date?: string;
} | null> {
  // If it looks like a UUID (job ID from Restate), use directly; otherwise generate from workflow ID
  const isJobId = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(jobIdOrWorkflowId);
  const jobId = isJobId ? jobIdOrWorkflowId : generateJobId(jobIdOrWorkflowId);
  let db = null;

  try {
    db = await connectMotherduck();

    // Get job info
    const jobResult = await executeQuery<{
      status: string;
      extract_number: number;
      snapshot_date: string;
    }>(db, `
      SELECT status, extract_number, snapshot_date
      FROM import_jobs
      WHERE id = '${jobId}'
    `);

    if (jobResult.length === 0) {
      return null;
    }

    const job = jobResult[0];

    // Get staging table counts
    const stagingTables = [
      'import_staging_activities',
      'import_staging_addresses',
      'import_staging_branches',
      'import_staging_contacts',
      'import_staging_denominations',
      'import_staging_enterprises',
      'import_staging_establishments',
    ];

    const staging_counts: Record<string, number> = {};

    for (const table of stagingTables) {
      const countResult = await executeQuery<{ count: number }>(db, `
        SELECT COUNT(*) as count FROM ${table} WHERE job_id = '${jobId}'
      `);
      const count = Number(countResult[0]?.count || 0);
      if (count > 0) {
        // Extract table name (e.g., "activities" from "import_staging_activities")
        const tableName = table.replace('import_staging_', '');
        staging_counts[tableName] = count;
      }
    }

    // Convert snapshot_date to string (DuckDB returns date objects)
    let snapshotDateStr: string | undefined;
    if (job.snapshot_date) {
      if (typeof job.snapshot_date === 'object' && 'days' in job.snapshot_date) {
        // Convert days since epoch to date string
        const date = new Date((job.snapshot_date as { days: number }).days * 24 * 60 * 60 * 1000);
        snapshotDateStr = date.toISOString().split('T')[0];
      } else {
        snapshotDateStr = String(job.snapshot_date);
      }
    }

    return {
      job_status: job.status,
      staging_counts,
      extract_number: job.extract_number,
      snapshot_date: snapshotDateStr,
    };
  } catch (error) {
    console.error("Error querying preparation progress:", error);
    return null;
  } finally {
    if (db) {
      await closeMotherduck(db);
    }
  }
}

export async function GET(
  request: Request,
  { params }: { params: Promise<{ workflowId: string }> }
) {
  try {
    const { workflowId } = await params;

    // Call shared handler to get progress
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (RESTATE_AUTH_TOKEN) {
      headers["Authorization"] = `Bearer ${RESTATE_AUTH_TOKEN}`;
    }

    const response = await fetch(
      `${RESTATE_INGRESS_URL}/KboImport/${workflowId}/getProgress`,
      {
        method: "POST",
        headers,
        body: "null",
      }
    );

    if (!response.ok) {
      // Check if workflow doesn't exist
      if (response.status === 404) {
        return NextResponse.json(
          { error: "Workflow not found", workflow_id: workflowId },
          { status: 404 }
        );
      }

      const errorText = await response.text();
      console.error("Restate error:", errorText);
      return NextResponse.json(
        { error: "Failed to get workflow status", details: errorText },
        { status: 500 }
      );
    }

    const progress = await response.json();

    // If in preparing state, enrich with database progress
    // Use job_id from Restate if available (more reliable), fall back to generating from workflowId
    if (progress.status === "preparing") {
      const prepProgress = await getPreparationProgress(progress.job_id || workflowId);
      if (prepProgress) {
        return NextResponse.json({
          workflow_id: workflowId,
          ...progress,
          preparation: prepProgress,
        });
      }
    }

    return NextResponse.json({
      workflow_id: workflowId,
      ...progress,
    });
  } catch (error) {
    console.error("Error getting workflow status:", error);
    return NextResponse.json(
      { error: "Failed to get workflow status", details: String(error) },
      { status: 500 }
    );
  }
}
