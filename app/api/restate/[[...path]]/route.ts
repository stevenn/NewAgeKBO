/**
 * Restate HTTP Handler Endpoint
 *
 * This endpoint is called BY Restate Server to execute workflow handlers.
 * Register this endpoint with Restate using:
 *   restate deployments register https://your-app.vercel.app/api/restate
 */

import * as restate from "@restatedev/restate-sdk/fetch";
import kboImportWorkflow from "@/lib/restate/kbo-import-service";

// Create Restate endpoint that Restate Server will call
const handler = restate
  .endpoint()
  .bind(kboImportWorkflow)
  .handler();

// Export handlers for Next.js App Router
export const POST = handler.fetch;

// Restate also needs to discover available services
export const GET = handler.fetch;
