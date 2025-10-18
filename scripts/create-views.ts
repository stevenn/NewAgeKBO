#!/usr/bin/env tsx

/**
 * Create database views in Motherduck
 * Creates the *_current views that filter _is_current = true
 */

// Load environment variables (.env.local takes precedence, then .env)
import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import {
  connectMotherduck,
  closeMotherduck,
  executeStatement,
  getMotherduckConfig,
} from '../lib/motherduck'
import { formatUserError } from '../lib/errors'

const VIEWS = [
  {
    name: 'enterprises_current',
    sql: 'CREATE OR REPLACE VIEW enterprises_current AS SELECT * FROM enterprises WHERE _is_current = true',
  },
  {
    name: 'establishments_current',
    sql: 'CREATE OR REPLACE VIEW establishments_current AS SELECT * FROM establishments WHERE _is_current = true',
  },
  {
    name: 'denominations_current',
    sql: 'CREATE OR REPLACE VIEW denominations_current AS SELECT * FROM denominations WHERE _is_current = true',
  },
  {
    name: 'addresses_current',
    sql: 'CREATE OR REPLACE VIEW addresses_current AS SELECT * FROM addresses WHERE _is_current = true',
  },
  {
    name: 'activities_current',
    sql: 'CREATE OR REPLACE VIEW activities_current AS SELECT * FROM activities WHERE _is_current = true',
  },
  {
    name: 'contacts_current',
    sql: 'CREATE OR REPLACE VIEW contacts_current AS SELECT * FROM contacts WHERE _is_current = true',
  },
  {
    name: 'branches_current',
    sql: 'CREATE OR REPLACE VIEW branches_current AS SELECT * FROM branches WHERE _is_current = true',
  },
]

async function createViews() {
  console.log('👁️  Creating database views...\n')

  try {
    // Step 1: Connect
    console.log('1️⃣  Connecting to Motherduck...')
    const mdConfig = getMotherduckConfig()
    const db = await connectMotherduck()
    console.log('   ✅ Connected successfully!\n')

    // Step 2: Use database
    console.log('2️⃣  Using database...')
    await executeStatement(db, `USE ${mdConfig.database}`)
    console.log(`   ✅ Using database "${mdConfig.database}"\n`)

    // Step 3: Create views
    console.log('3️⃣  Creating views...')
    let viewsCreated = 0

    for (const view of VIEWS) {
      try {
        await executeStatement(db, view.sql)
        console.log(`   ✅ Created view: ${view.name}`)
        viewsCreated++
      } catch (error) {
        console.error(`   ❌ Failed to create view: ${view.name}`)
        if (error instanceof Error) {
          console.error(`      Error: ${error.message}`)
        }
      }
    }

    console.log(`\n   ✅ Created ${viewsCreated}/${VIEWS.length} views\n`)

    // Close connection
    await closeMotherduck(db)

    // Success summary
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    console.log('✨ SUCCESS! Database views are ready')
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n')

    console.log('📊 Views Created:')
    for (const view of VIEWS) {
      console.log(`   • ${view.name}`)
    }
    console.log()

    console.log('💡 These views filter _is_current = true for faster queries')
    console.log()
  } catch (error) {
    console.error('\n❌ View creation failed!\n')

    if (error instanceof Error) {
      console.error(`Error: ${formatUserError(error)}\n`)

      console.error('💡 Troubleshooting:')
      console.error('   1. Check Motherduck connection')
      console.error('   2. Verify database and tables exist')
      console.error('   3. See docs/MOTHERDUCK_SETUP.md\n')

      if (process.env.NODE_ENV === 'development') {
        console.error('Stack trace:')
        console.error(error.stack)
      }
    }

    process.exit(1)
  }
}

// Run the view creation
createViews()
