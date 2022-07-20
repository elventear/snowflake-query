import * as core from '@actions/core'
import {ConnectionOptions, createConnection} from 'snowflake-sdk'
import {handleConnection} from './snowflake'

interface Inputs {
  readonly account: string
  readonly database: string
  readonly password: string
  readonly privkey_pass: string
  readonly privkey_path: string
  readonly query: string
  readonly role: string
  readonly schema: string
  readonly username: string
  readonly warehouse: string
}

interface Outputs {
  readonly rows: string
}

export const INPUTS: Inputs = {
  account: 'account',
  database: 'database',
  password: 'password',
  privkey_pass: 'private_key_pass',
  privkey_path: 'private_key_path',
  query: 'query',
  role: 'role',
  schema: 'schema',
  username: 'username',
  warehouse: 'warehouse'
}

export const OUTPUTS: Outputs = {
  rows: 'rows'
}

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
    account: requiredInput(INPUTS.account),
    database: requiredInput(INPUTS.database),
    password: optionalInput(INPUTS.password),
    privateKeyPath: optionalInput(INPUTS.privkey_path),
    privateKeyPass: optionalInput(INPUTS.privkey_pass),
    role: optionalInput(INPUTS.role),
    schema: requiredInput(INPUTS.schema),
    username: requiredInput(INPUTS.username),
    warehouse: requiredInput(INPUTS.warehouse),
    clientSessionKeepAlive: false
  }

  const queries = core.getMultilineInput(INPUTS.query, {required: true})

  if (!options.password && !options.privateKeyPass && !options.privateKeyPath) {
    core.setFailed('Password or private key authentication required')
    return
  }

  if (options.privateKeyPath && options.privateKeyPass) {
    options.authenticator = 'SNOWFLAKE_JWT'
  } else {
    options.authenticator = 'SNOWFLAKE'
  }

  createConnection(options).connect((err, conn) => {
    if (err) {
      core.setFailed(`Connection failure: ${err.message}`)
    } else {
      handleConnection(conn, queries)
    }
  })
}

runSnowflake()
