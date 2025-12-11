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
const endpoint = restate.endpoint().bind(kboImportWorkflow);

// Enable request signature validation if public key is configured
if (process.env.RESTATE_SIGNING_PUBLIC_KEY) {
  endpoint.withIdentityV1(process.env.RESTATE_SIGNING_PUBLIC_KEY);
}

const handler = endpoint.handler();

// Export handlers for Next.js App Router
export const POST = handler.fetch;

// Restate also needs to discover available services
export const GET = handler.fetch;
