/**
 * Get KBO Import Workflow Status
 *
 * GET /api/admin/imports/[workflowId]/status
 *
 * Fetches progress from Restate for a running or completed import workflow.
 */

import { NextResponse } from "next/server";

const RESTATE_INGRESS_URL = process.env.RESTATE_INGRESS_URL || "http://localhost:8080";

export async function GET(
  request: Request,
  { params }: { params: Promise<{ workflowId: string }> }
) {
  try {
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
