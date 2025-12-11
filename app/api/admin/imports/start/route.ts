/**
 * Start KBO Import Workflow
 *
 * POST /api/admin/imports/start
 * Body: { fileUrl: string, filename: string }
 *
 * Triggers a durable Restate workflow to download and process the KBO update.
 */

import { NextResponse } from "next/server";

const RESTATE_INGRESS_URL = process.env.RESTATE_INGRESS_URL || "http://localhost:8080";

export async function POST(request: Request) {
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

    // Start workflow via Restate ingress (fire-and-forget with /send)
    const response = await fetch(
      `${RESTATE_INGRESS_URL}/KboImport/${workflowId}/run/send`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ fileUrl, filename }),
      }
    );

    if (!response.ok) {
      const errorText = await response.text();
      console.error("Restate error:", errorText);
      return NextResponse.json(
        { error: "Failed to start workflow", details: errorText },
        { status: 500 }
      );
    }

    return NextResponse.json({
      workflow_id: workflowId,
      status: "started",
      message: `Import workflow ${workflowId} started for ${filename}`,
    });
  } catch (error) {
    console.error("Error starting import:", error);
    return NextResponse.json(
      { error: "Failed to start import", details: String(error) },
      { status: 500 }
    );
  }
}
