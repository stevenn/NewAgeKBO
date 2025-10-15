/**
 * Import job metadata types
 */

export enum ImportJobStatus {
  Pending = 'pending',
  Running = 'running',
  Completed = 'completed',
  Failed = 'failed',
}

export type ImportJobType = 'full' | 'update'

export type WorkerType = 'local' | 'vercel'

export interface ImportJob {
  id: string // UUID
  extract_number: number
  extract_type: ImportJobType
  snapshot_date: Date
  extract_timestamp: Date
  status: ImportJobStatus
  started_at: Date | null
  completed_at: Date | null
  error_message: string | null
  records_processed: number
  records_inserted: number
  records_updated: number
  records_deleted: number
  worker_type: WorkerType
}

export interface MetaData {
  snapshot_date: Date
  extract_timestamp: Date
  extract_number: number
  extract_type: ImportJobType
  version: string
}
