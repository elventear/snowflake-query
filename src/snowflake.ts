import * as core from '@actions/core'
import {Connection, StatementStatus} from 'snowflake-sdk'
import {OUTPUTS} from './main'

const sleep = async (waitTime: number): Promise<void> =>
  new Promise(resolve => setTimeout(resolve, waitTime))

export async function handleConnection(
  connection: Connection,
  queries: string[]
): Promise<void> {
  core.startGroup('Snowflake queries')

  const lastRows: unknown[] = []
  let failed = false
  for (const query of queries) {
    if (failed) {
      break
    }
    let finished = false
    // clear array
    lastRows.length = 0

    core.info(`Running query: ${query}`)
    const statement = connection.execute({
      sqlText: query,
      streamResult: true
    })

    statement
      .streamRows()
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
      .on('data', function (row) {
        lastRows.push(row)
        if (core.isDebug()) {
          core.debug(`Row: ${row}`)
        }
      })
      .on('end', function () {
        finished = true
      })

    while (!failed && !finished) {
      await sleep(500)
    }
  }
  core.endGroup()

  core.setOutput(OUTPUTS.rows, lastRows)
}
