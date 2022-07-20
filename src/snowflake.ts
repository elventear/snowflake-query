import * as core from '@actions/core'
import {Connection, StatementStatus} from 'snowflake-sdk'
import {OUTPUTS} from './main'

const sleep = async (waitTime: number): Promise<void> =>
  new Promise(resolve => setTimeout(resolve, waitTime))

export async function handleConnection(
  connection: Connection,
  queries: string[]
): Promise<void> {
  core.startGroup('Snowlflake Queries')

  const lastRows: unknown[] = []
  let failed = false
  for (const query of queries) {
    if (failed) {
      break
    }

    core.info(`Running query: ${query}`)
    const statement = connection.execute({
      sqlText: query,
      streamResult: true,
      complete(err, stmt) {
        if (err) {
          core.setFailed(`Query error: ${err.message}`)
          return
        }

        stmt
          .streamRows()
          .on('data', function (row) {
            lastRows.push(row)
            if (core.isDebug()) {
              core.debug(`Row: ${row}`)
            }
          })
          // eslint-disable-next-line no-shadow
          .on('error', function (err) {
            failed = true
            core.setFailed(`Error consuming rows: ${err.message}`)
            // eslint-disable-next-line no-shadow
            statement.cancel(function (err) {
              if (err) {
                core.error(`Failed to cancel statement: ${err.message}`)
              }
            })
          })
      }
    })

    while (!failed && statement.getStatus() !== StatementStatus.Complete) {
      await sleep(500)
    }

    // clear array
    lastRows.length = 0
  }
  core.endGroup()

  core.setOutput(OUTPUTS.rows, lastRows)
}
