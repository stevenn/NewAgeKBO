/**
 * KBO Import Workflow - Restate Durable Execution
 *
 * Wraps the existing batch import system in Restate for durability.
 * Each step is checkpointed, so if Vercel times out or crashes,
 * Restate will resume from the last completed step.
 */

import * as restate from "@restatedev/restate-sdk";
import {
  prepareImport,
  processBatch,
  finalizeImport,
  type PrepareImportResult,
} from "@/lib/import/batched-update";
import { downloadFile } from "@/lib/kbo-client";

/**
 * Progress state stored in Restate
 */
interface ImportProgress {
  status: "pending" | "downloading" | "preparing" | "processing" | "finalizing" | "completed" | "failed";
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
  fileUrl: string;
  filename: string;
}

/**
 * KBO Import Workflow
 *
 * Orchestrates the full import process:
 * 1. Download ZIP from KBO portal
 * 2. Prepare import (parse ZIP, populate staging tables, create batches)
 * 3. Process each batch (delete then insert operations)
 * 4. Finalize (resolve names, cleanup staging)
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
      const { fileUrl, filename } = input;

      // Initialize progress
      ctx.set("progress", {
        status: "downloading",
        completed_batches: 0,
        total_batches: 0,
      } as ImportProgress);

      // Step 1: Download ZIP (durable, retried on failure)
      ctx.console.log(`Downloading ${filename}...`);
      const zipBuffer = await ctx.run("download-zip", async () => {
        const buffer = await downloadFile(fileUrl);
        // Return as base64 since Restate serializes state as JSON
        return buffer.toString("base64");
      });

      // Step 2: Prepare import (parse ZIP, create batches)
      ctx.set("progress", {
        status: "preparing",
        completed_batches: 0,
        total_batches: 0,
      } as ImportProgress);

      ctx.console.log("Preparing import...");
      const prepareResult = await ctx.run(
        "prepare-import",
        async (): Promise<PrepareImportResult> => {
          const buffer = Buffer.from(zipBuffer, "base64");
          return await prepareImport(buffer, "vercel");
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

      // Step 3: Process each batch (each is a durable checkpoint)
      // Order: deletes first, then inserts, for each table
      let completedBatches = 0;

      for (const [table, counts] of Object.entries(batches_by_table)) {
        // Process DELETE batches first
        for (let i = 1; i <= counts.delete; i++) {
          ctx.console.log(`Processing ${table} delete batch ${i}/${counts.delete}`);

          await ctx.run(`delete-${table}-${i}`, async () => {
            await processBatch(job_id, table, i);
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

          // Batch number continues after deletes
          const batchNum = counts.delete + i;
          await ctx.run(`insert-${table}-${i}`, async () => {
            await processBatch(job_id, table, batchNum);
          });

          completedBatches++;
          ctx.set("progress", {
            status: "processing",
            job_id,
            completed_batches: completedBatches,
            total_batches,
            current_table: table,
            current_batch: batchNum,
          } as ImportProgress);
        }
      }

      // Step 4: Finalize
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
