/**
 * KBO Import Workflow - Restate Durable Execution
 *
 * Wraps the existing batch import system in Restate for durability.
 * Each step is checkpointed, so if Vercel times out or crashes,
 * Restate will resume from the last completed step.
 *
 * Note: Large files are stored in Vercel Blob and only URLs are passed through
 * Restate to avoid payload size limits.
 */

import * as restate from "@restatedev/restate-sdk";
import {
  prepareImport,
  processBatch,
  finalizeImport,
  type PrepareImportResult,
} from "@/lib/import/batched-update";
import { deleteFromBlob } from "@/lib/blob";

/**
 * Progress state stored in Restate
 */
interface ImportProgress {
  status: "pending" | "preparing" | "processing" | "finalizing" | "completed" | "failed";
  job_id?: string;
  completed_batches: number;
  total_batches: number;
  current_table?: string;
  current_batch?: number;
  error?: string;
}

/**
 * Input for the import workflow
 */
interface ImportInput {
  blobUrl: string;  // Vercel Blob URL (not KBO URL)
  filename: string;
}

/**
 * KBO Import Workflow
 *
 * Orchestrates the full import process:
 * 1. Prepare import (download from blob, parse ZIP, populate staging tables, create batches)
 * 2. Process each batch (delete then insert operations)
 * 3. Finalize (resolve names, cleanup staging)
 * 4. Cleanup blob storage
 */
const kboImportWorkflow = restate.workflow({
  name: "KboImport",
  handlers: {
    /**
     * Main workflow handler - runs exactly once per workflow ID
     */
    run: async (
      ctx: restate.WorkflowContext,
      input: ImportInput
    ): Promise<{ job_id: string; names_resolved: number }> => {
      const { blobUrl, filename } = input;

      // Store blob URL for cleanup in case of failure
      ctx.set("blobUrl", blobUrl);

      // Initialize progress
      ctx.set("progress", {
        status: "preparing",
        completed_batches: 0,
        total_batches: 0,
      } as ImportProgress);

      try {
        // Step 1: Prepare import (downloads from blob, parses ZIP, creates batches)
        // prepareImport handles the blob download internally
        ctx.console.log(`Preparing import from blob for ${filename}...`);
        const prepareResult = await ctx.run(
          "prepare-import",
          async (): Promise<PrepareImportResult> => {
            return await prepareImport(blobUrl, "vercel");
          }
        );

        const { job_id, batches_by_table, total_batches } = prepareResult;

        ctx.console.log(
          `Import prepared: job_id=${job_id}, total_batches=${total_batches}`
        );

        // Update progress with batch info
        ctx.set("progress", {
          status: "processing",
          job_id,
          completed_batches: 0,
          total_batches,
        } as ImportProgress);

        // Step 2: Process each batch (each is a durable checkpoint)
        // Order: deletes first, then inserts, for each table
        let completedBatches = 0;

        for (const [table, counts] of Object.entries(batches_by_table)) {
          // Process DELETE batches first
          for (let i = 1; i <= counts.delete; i++) {
            ctx.console.log(`Processing ${table} delete batch ${i}/${counts.delete}`);

            await ctx.run(`delete-${table}-${i}`, async () => {
              await processBatch(job_id, table, i, 'delete');
            });

            completedBatches++;
            ctx.set("progress", {
              status: "processing",
              job_id,
              completed_batches: completedBatches,
              total_batches,
              current_table: table,
              current_batch: i,
            } as ImportProgress);
          }

          // Then process INSERT batches
          for (let i = 1; i <= counts.insert; i++) {
            ctx.console.log(`Processing ${table} insert batch ${i}/${counts.insert}`);

            await ctx.run(`insert-${table}-${i}`, async () => {
              await processBatch(job_id, table, i, 'insert');
            });

            completedBatches++;
            ctx.set("progress", {
              status: "processing",
              job_id,
              completed_batches: completedBatches,
              total_batches,
              current_table: table,
              current_batch: i,
            } as ImportProgress);
          }
        }

        // Step 3: Finalize
        ctx.set("progress", {
          status: "finalizing",
          job_id,
          completed_batches: total_batches,
          total_batches,
        } as ImportProgress);

        ctx.console.log("Finalizing import...");
        const finalResult = await ctx.run("finalize", async () => {
          return await finalizeImport(job_id);
        });

        // Step 4: Cleanup blob storage
        ctx.console.log("Cleaning up blob storage...");
        await ctx.run("cleanup-blob", async () => {
          await deleteFromBlob(blobUrl);
        });

        // Mark as completed
        ctx.set("progress", {
          status: "completed",
          job_id,
          completed_batches: total_batches,
          total_batches,
        } as ImportProgress);

        ctx.console.log(
          `Import completed: ${finalResult.names_resolved} names resolved`
        );

        return {
          job_id,
          names_resolved: finalResult.names_resolved,
        };
      } catch (error) {
        // On any error, attempt to cleanup the blob
        ctx.console.error(`Workflow error: ${error}`);

        // Mark as failed
        const currentProgress = await ctx.get<ImportProgress>("progress");
        ctx.set("progress", {
          ...currentProgress,
          status: "failed",
          error: String(error),
        } as ImportProgress);

        // Try to cleanup blob (best effort - don't fail if cleanup fails)
        try {
          await ctx.run("cleanup-blob-on-error", async () => {
            const url = blobUrl;
            if (url) {
              await deleteFromBlob(url);
            }
          });
        } catch (cleanupError) {
          ctx.console.error(`Failed to cleanup blob: ${cleanupError}`);
        }

        throw error;
      }
    },

    /**
     * Shared handler to get progress (can be called while workflow runs)
     */
    getProgress: restate.handlers.workflow.shared(
      async (ctx: restate.WorkflowSharedContext): Promise<ImportProgress> => {
        return (
          (await ctx.get<ImportProgress>("progress")) || {
            status: "pending",
            completed_batches: 0,
            total_batches: 0,
          }
        );
      }
    ),
  },
});

export default kboImportWorkflow;
