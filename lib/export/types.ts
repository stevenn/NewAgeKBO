/**
 * TypeScript types for export jobs
 */

export type ExportType = 'vat_entities'

export type ExportStatus = 'pending' | 'running' | 'completed' | 'failed'

export type WorkerType = 'vercel' | 'web_manual' | 'cli'

export interface ExportJob {
  id: string
  export_type: ExportType
  filter_config: Record<string, unknown> | null
  status: ExportStatus
  started_at: string | null
  completed_at: string | null
  error_message: string | null
  records_exported: number
  table_name: string | null
  expires_at: string | null
  worker_type: WorkerType
  created_by: string | null
  created_at: string
}

export interface ExportVatEntitiesResult {
  job_id: string
  table_name: string
  records_exported: number
  expires_at: string
}

export interface ExportJobsListResponse {
  jobs: ExportJob[]
  total: number
  page: number
  totalPages: number
}

export interface VatEntityRow {
  entity_number: string
  juridical_form: string | null
  status: string
  activity_groups: string
}
