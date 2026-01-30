/**
 * Start KBO Import Workflow
 *
 * POST /api/admin/imports/start
 * Body: { fileUrl: string, filename: string }
 *
 * Downloads the KBO file, uploads to Vercel Blob, then triggers a durable
 * Restate workflow to process it. This keeps large payloads out of Restate's
 * state journal.
 */

import { NextResponse } from "next/server";
import { downloadFile } from "@/lib/kbo-client";
import { uploadToBlob, deleteFromBlob } from "@/lib/blob";

const RESTATE_INGRESS_URL = process.env.RESTATE_INGRESS_URL || "http://localhost:8080";
const RESTATE_ADMIN_URL = process.env.RESTATE_ADMIN_URL || "http://localhost:9070";
const RESTATE_AUTH_TOKEN = process.env.RESTATE_AUTH_TOKEN;

export async function POST(request: Request) {
  let blobUrl: string | null = null;

  try {
    const { fileUrl, filename } = await request.json();

    if (!fileUrl || !filename) {
      return NextResponse.json(
        { error: "Missing fileUrl or filename" },
        { status: 400 }
      );
    }

    // Generate workflow ID from filename (e.g., "import-0141" from "KboOpenData_0141_...")
    const extractMatch = filename.match(/KboOpenData_(\d+)_/);
    const workflowId = extractMatch
      ? `import-${extractMatch[1]}`
      : `import-${Date.now()}`;

    // Step 1: Download from KBO portal (happens in API route, not in Restate)
    console.log(`Downloading ${filename} from KBO portal...`);
    const zipBuffer = await downloadFile(fileUrl);
    console.log(`Downloaded ${zipBuffer.length} bytes`);

    // Step 2: Upload to Vercel Blob (keeps large file out of Restate state)
    console.log(`Uploading to Vercel Blob...`);
    const blob = await uploadToBlob(zipBuffer, filename, workflowId);
    blobUrl = blob.url;
    console.log(`Uploaded to blob: ${blob.pathname} (${blob.size} bytes)`);

    // Step 3: Purge any existing workflow with the same ID (allows re-imports)
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (RESTATE_AUTH_TOKEN) {
      headers["Authorization"] = `Bearer ${RESTATE_AUTH_TOKEN}`;
    }

    console.log(`Purging any existing workflow: ${workflowId}`);
    try {
      const purgeResponse = await fetch(
        `${RESTATE_ADMIN_URL}/restate/workflow/KboImport/${workflowId}/purge`,
        { method: "DELETE", headers }
      );
      if (purgeResponse.ok) {
        console.log(`Purged existing workflow: ${workflowId}`);
        // Small delay to ensure purge is processed
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    } catch (purgeError) {
      // Ignore purge errors - workflow might not exist
      console.log(`Purge skipped (workflow may not exist): ${purgeError}`);
    }

    // Step 4: Start workflow via Restate ingress (fire-and-forget with /send)
    // Only pass the blob URL - Restate never sees the large file
    const response = await fetch(
      `${RESTATE_INGRESS_URL}/KboImport/${workflowId}/run/send`,
      {
        method: "POST",
        headers,
        body: JSON.stringify({ blobUrl: blob.url, filename }),
      }
    );

    if (!response.ok) {
      const errorText = await response.text();
      console.error("Restate error:", errorText);

      // Clean up blob since workflow won't run
      if (blobUrl) {
        console.log("Cleaning up blob after Restate start failure...");
        await deleteFromBlob(blobUrl);
      }

      return NextResponse.json(
        { error: "Failed to start workflow", details: errorText },
        { status: 500 }
      );
    }

    return NextResponse.json({
      workflow_id: workflowId,
      status: "started",
      message: `Import workflow ${workflowId} started for ${filename}`,
      blob_url: blob.url,
    });
  } catch (error) {
    console.error("Error starting import:", error);

    // Clean up blob if it was created but something failed
    if (blobUrl) {
      console.log("Cleaning up blob after error...");
      try {
        await deleteFromBlob(blobUrl);
      } catch (cleanupError) {
        console.error("Failed to cleanup blob:", cleanupError);
      }
    }

    return NextResponse.json(
      { error: "Failed to start import", details: String(error) },
      { status: 500 }
    );
  }
}
