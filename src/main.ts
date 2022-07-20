import * as core from '@actions/core'
import {ConnectionOptions, createConnection} from 'snowflake-sdk'
import {handleConnection} from './snowflake'

const ACCOUNT = 'account'
const DATABASE = 'database'
const PASSWORD = 'password'
const PRIVATE_KEY_PATH = 'private-key-path'
const PRIVATE_KEY_PASS = 'private-key-pass'
const ROLE = 'role'
const SCHEMA = 'schema'
const USERNAME = 'username'
const WAREHOUSE = 'warehouse'
const QUERY = 'query'

function requiredInput(name: string): string {
  return core.getInput(name, {trimWhitespace: true, required: true})
}

function optionalInput(name: string): string | undefined {
  const input = core.getInput(name, {trimWhitespace: true, required: false})
  if (input) {
    return input
  }

  return undefined
}

async function runSnowflake(): Promise<void> {
  const options: ConnectionOptions = {
    account: requiredInput(ACCOUNT),
    database: requiredInput(DATABASE),
    password: optionalInput(PASSWORD),
    privateKeyPath: optionalInput(PRIVATE_KEY_PATH),
    privateKeyPass: optionalInput(PRIVATE_KEY_PASS),
    role: optionalInput(ROLE),
    schema: requiredInput(SCHEMA),
    username: requiredInput(USERNAME),
    warehouse: requiredInput(WAREHOUSE),
    clientSessionKeepAlive: false
  }

  const queries = core.getMultilineInput(QUERY, {required: true})

  if (!options.password && !options.privateKeyPass && !options.privateKeyPath) {
    core.setFailed('Password or private key authentication required')
    return
  }

  await createConnection(options).connectAsync(async (err, conn) => {
    if (err) {
      core.setFailed(`Connection failure: ${err.message}`)
    } else {
      await handleConnection(conn, queries)
    }
  })
}

runSnowflake()
