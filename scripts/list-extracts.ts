#!/usr/bin/env tsx

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { connectMotherduck, executeQuery } from '../lib/motherduck'

async function list() {
  const db = await connectMotherduck()
  await executeQuery(db, `USE ${process.env.MOTHERDUCK_DATABASE}`)

  const result = await executeQuery(db, `
    SELECT DISTINCT _extract_number
    FROM enterprises
    ORDER BY _extract_number
  `)

  console.log('Extract numbers in database:')
  console.log(result.map(r => r._extract_number).join(', '))
}

list()
