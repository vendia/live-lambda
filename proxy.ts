import { DescribeEndpointCommand, IoTClient } from '@aws-sdk/client-iot'
import {
  GetFunctionConfigurationCommand,
  GetFunctionConfigurationCommandOutput,
  LambdaClient,
  UpdateFunctionCodeCommand,
  UpdateFunctionConfigurationCommand,
  UpdateFunctionConfigurationCommandInput,
} from '@aws-sdk/client-lambda'
import { toUtf8 } from '@aws-sdk/util-utf8-browser'
import * as archiver from 'archiver'
import { iot, mqtt5 } from 'aws-iot-device-sdk-v2'
import { exec } from 'child_process'
import * as crypto from 'crypto'
import * as esbuild from 'esbuild'
import { once } from 'events'
import * as fs from 'fs'
import * as path from 'path'
import { python } from 'pythonia'
import { Argv } from 'yargs'

import { fetchUniByName } from '../../../../api/src/unis/unis.service'
import { GlobalArgs } from '../common/constants'

export const command = 'proxy'
export const description = `Proxy requests from a lambda to a local file`

interface Args extends GlobalArgs {
  lambda?: string
  file?: string
  plane?: 'control' | 'data'
  uni?: string
  node?: string
  skipDeploy?: boolean
}

export const builder = (yargs: Argv<Args>) => {
  return yargs
    .option('uni', {
      describe: 'For data plane operations, this is the desired Uni to use',
      type: 'string',
      demandOption: false,
      alias: 'u',
    })
    .option('node', {
      describe: 'For data plane operations, this is the desired Node to use',
      type: 'string',
      demandOption: false,
      alias: 'n',
    })
    .option('lambda', {
      describe:
        'The lambda function to replace with the local file. This can either be the full function name (eg. VendiaShareDeployer) or the function alias for data plane operations (eg. graphql).',
      type: 'string',
      demandOption: false,
      alias: 'l',
    })
    .option('file', {
      describe: 'The path to the local file relative to the current working directory',
      type: 'string',
      demandOption: false,
      alias: 'f',
    })
    .option('plane', {
      describe: 'The context in which to execute the proxy',
      type: 'string',
      demandOption: false,
      choices: ['control', 'data'],
      alias: 'p',
    })
    .option('skip-deploy', {
      describe: 'Skip the deployment of the proxy',
      type: 'boolean',
      demandOption: false,
      alias: 's',
    })
}

const invokeByPath = async (path: string, event: any, context: any) => {
  const { handler: fn } = await import(`${path}?cacheBust=${Number(new Date())}`)
  const response = await fn(event, context)
  for (const path in require.cache) {
    if (path.endsWith('.js')) {
      // only clear *.js, not *.node
      delete require.cache[path]
    }
  }
  return response
}

const getIotWebsocketEndpoint = async (region: string) => {
  const iot = new IoTClient({
    region,
  })
  const params = {
    endpointType: 'iot:Data-ATS',
  }
  return iot.send(new DescribeEndpointCommand(params))
}

const creatClientConfig = ({ websocketEndpoint, region }: { websocketEndpoint: string; region: string }) => {
  const builder = iot.AwsIotMqtt5ClientConfigBuilder.newWebsocketMqttBuilderWithSigv4Auth(websocketEndpoint, {
    region,
  })
  builder.withConnectProperties({
    keepAliveIntervalSeconds: 1200,
  })
  return builder.build()
}

const createMqttClient = ({
  websocketEndpoint,
  region,
}: {
  websocketEndpoint: string
  region: string
}): mqtt5.Mqtt5Client => {
  const config: mqtt5.Mqtt5ClientConfig = creatClientConfig({
    websocketEndpoint,
    region,
  })
  return new mqtt5.Mqtt5Client(config)
}

const updateLambdaFunctionCode = async (functionName: string, zipFile: Buffer, region: string) => {
  const lambdaClient = new LambdaClient({ region })
  const params = {
    FunctionName: functionName,
    ZipFile: zipFile,
  }
  const command = new UpdateFunctionCodeCommand(params)
  return lambdaClient.send(command)
}

const updateLambdaFunctionConfiguration = async ({
  functionName,
  region,
  runtime,
  handler,
}: {
  functionName: string
  region: string
  runtime: string
  handler: string
}) => {
  const lambdaClient = new LambdaClient({ region })
  const params: UpdateFunctionConfigurationCommandInput = {
    // @ts-ignore
    Runtime: runtime,
    Handler: handler,
    FunctionName: functionName,
  }
  const command = new UpdateFunctionConfigurationCommand(params)
  return lambdaClient.send(command)
}

const getLambdaConfiguration = async (functionName: string, region: string) => {
  const lambdaClient = new LambdaClient({ region })
  const params = {
    FunctionName: functionName,
  }
  const command = new GetFunctionConfigurationCommand(params)
  return lambdaClient.send(command)
}

const copyDirectorySync = (source: string, target: string) => {
  fs.readdirSync(source).forEach((file) => {
    const sourceFile = path.join(source, file)
    const targetFile = path.join(target, file)

    if (fs.lstatSync(sourceFile).isDirectory()) {
      if (!fs.existsSync(targetFile)) {
        fs.mkdirSync(targetFile)
      }
      copyDirectorySync(sourceFile, targetFile)
    } else {
      fs.copyFileSync(sourceFile, targetFile)
    }
  })
}

const zipDirectory = (source: string, out: string) => {
  const archive = archiver.default('zip', { zlib: { level: 9 } })
  const stream = fs.createWriteStream(out)

  return new Promise((resolve, reject) => {
    archive
      .directory(source, false)
      .on('error', (err: any) => reject(err))
      .pipe(stream)

    stream.on('close', () => resolve(''))
    archive.finalize()
  })
}

const hashDirectory = (directory: string) => {
  const hash = crypto.createHash('sha256')
  const files = fs.readdirSync(directory)

  for (const file of files) {
    const filePath = path.join(directory, file)
    const stat = fs.statSync(filePath)

    if (stat.isFile()) {
      const fileData = fs.readFileSync(filePath)
      hash.update(fileData)
    } else if (stat.isDirectory()) {
      const nestedHash = hashDirectory(filePath)
      hash.update(nestedHash)
    }
  }

  return hash.digest('hex')
}

const runNpmInstall = async (directory: string): Promise<void> => {
  const { consola } = await import('consola')
  return new Promise((resolve, reject) => {
    exec('npm install', { cwd: directory }, (error, stdout, stderr) => {
      if (error) {
        consola.fatal(`Error running npm install: ${error}`)
        reject(error)
        return
      }
      consola.debug(`npm install output: ${stdout}`)
      resolve()
    })
  })
}

const runRustBinary = async ({
  directory,
  fileName,
  event,
  context,
}: {
  directory: string
  fileName: string
  event: any
  context: any
}): Promise<string> => {
  const { consola } = await import('consola')
  return new Promise((resolve, reject) => {
    exec(
      `./${fileName} --event='${JSON.stringify(event)}' --context='${JSON.stringify(context)}'`,
      { cwd: directory },
      (error, stdout, stderr) => {
        if (error) {
          consola.fatal(`Error: ${error}`)
          reject(error)
          return
        }
        consola.info(`Output: ${stdout}`)
        resolve(stdout)
      },
    )
  })
}

const buildProxyHandler = async (handlerName: string, tmpDir: string): Promise<string> => {
  await esbuild.build({
    entryPoints: [path.resolve(__dirname, `../common/local_proxy/iot-publish.ts`)],
    outfile: path.resolve(tmpDir, `dist/${handlerName?.split('.')?.[0]}.js`),
    bundle: true,
    platform: 'node',
    target: 'node20',
    external: ['aws-crt'],
  })
  copyDirectorySync(path.resolve(__dirname, `../common/local_proxy`), path.resolve(tmpDir, 'dist'))
  await runNpmInstall(path.resolve(tmpDir, 'dist'))
  const hash = hashDirectory(path.resolve(tmpDir, 'dist'))
  const zipName = `asset.${hash}.zip`
  await zipDirectory(path.resolve(tmpDir, `dist`), path.resolve(tmpDir, zipName))
  return zipName
}

const exitHandler = async ({
  event,
  context,
}: {
  event: string
  context: {
    functionName: string
    region: string
    runtime: string
    tmpDir: string
  }
}) => {
  const { consola } = await import('consola')
  const { functionName, region, tmpDir, runtime } = context
  consola.warn(`Received ${event} event. Cleaning up...`)
  if (runtime !== 'nodejs20.x') {
    const filePath = path.resolve(tmpDir, 'backup', `${functionName}.json`)
    const lambdaConfiguration: GetFunctionConfigurationCommandOutput = JSON.parse(
      fs.readFileSync(filePath).toString(),
    ) as GetFunctionConfigurationCommandOutput
    await updateLambdaFunctionConfiguration({
      functionName,
      region,
      runtime: lambdaConfiguration.Runtime!,
      handler: lambdaConfiguration.Handler!,
    })
  }
  //@ts-ignore
  python.exit()
  fs.rmSync(tmpDir, { recursive: true })
  process.exit(0)
}

// Use the cli argument to invoke the local function
const run = async ({
  region,
  functionName,
  localPath,
  topic,
  skipDeploy,
}: {
  region: string
  functionName: string
  localPath: string
  topic: string
  skipDeploy?: boolean
}) => {
  // TODO: we should be able to import this normally, but commonjs seems to be broken in v3. We should fix this if they patch it.
  const { consola } = await import('consola')
  consola.start('Deploying local proxy...')

  const tmpdir = fs.mkdtempSync('tmp-proxy-')
  const fullyResolvedTmpDir = path.resolve(process.cwd(), tmpdir)

  consola.info('Fetching lambda configuration for', functionName)
  let lambdaConfig = await getLambdaConfiguration(functionName, region)
  if (lambdaConfig.Runtime !== 'nodejs20.x') {
    consola.info('Lambda function is not using nodejs20.x runtime')
    consola.info('Backing up lambda function configuration...')
    fs.mkdirSync(path.resolve(fullyResolvedTmpDir, 'backup'), { recursive: true })
    const backupFile = path.resolve(fullyResolvedTmpDir, `backup/${functionName}.json`)
    fs.writeFileSync(backupFile, JSON.stringify(lambdaConfig, null, 2))

    consola.info('Updating lambda function configuration to nodejs20.x')
    await updateLambdaFunctionConfiguration({ functionName, region, runtime: 'nodejs20.x', handler: 'index.handler' })
    lambdaConfig = await getLambdaConfiguration(functionName, region)
  }
  const handlerName = lambdaConfig.Handler!

  consola.info('Building local proxy with handler', handlerName)
  const zipName = await buildProxyHandler(handlerName, fullyResolvedTmpDir)

  consola.info('Swapping lambda function code with proxy...')
  if (!skipDeploy) {
    await updateLambdaFunctionCode(functionName, fs.readFileSync(path.resolve(fullyResolvedTmpDir, zipName)), region)
  }

  const env = lambdaConfig.Environment?.Variables!
  env['AWS_DEFAULT_REGION'] = region
  env['CONSENSUS_DISABLE_VERSIONING'] = 'y'
  env['RUST_LOG'] = 'info'
  for (const key in env) {
    process.env[key] = env?.[key]
  }
  consola.info('Lambda environment variables applied locally.')

  //get the path relative to the directory that the cli is run from
  const pathofFileRelativeToCli = path.join(process.cwd(), localPath)

  consola.info('Connecting to websocket endpoint...')
  const websocketEndpoint = await getIotWebsocketEndpoint(region)
  if (!websocketEndpoint.endpointAddress) {
    throw new Error('No websocket endpoint found')
  }

  const mqttClient: mqtt5.Mqtt5Client = createMqttClient({
    websocketEndpoint: websocketEndpoint.endpointAddress!,
    region,
  })

  const connectionSuccess = once(mqttClient, 'connectionSuccess')

  mqttClient.start()

  await connectionSuccess
  consola.success('Connected. Ready for requests...')

  await mqttClient.subscribe({
    subscriptions: [{ qos: mqtt5.QoS.AtLeastOnce, topicFilter: topic }],
  })

  mqttClient.on('messageReceived', async (eventData: mqtt5.MessageReceivedEvent) => {
    try {
      if (eventData.message.payload) {
        const payload = JSON.parse(toUtf8(new Uint8Array(eventData.message.payload as ArrayBuffer)))
        if (payload.event) {
          consola.box(`********** Received Event (${payload.context.awsRequestId}) **********`)
          let result

          if (!path.extname(pathofFileRelativeToCli)) {
            const directoryOfFile = path.dirname(pathofFileRelativeToCli)
            const fileName = path.basename(pathofFileRelativeToCli)
            const stdout = await runRustBinary({
              directory: directoryOfFile,
              fileName,
              event: payload.event,
              context: payload.context,
            })
            const regex = /Finished Local Invocation: (\{.*\})/
            const unparsedResult = stdout.match(regex)?.[1]
            if (!unparsedResult) {
              consola.warn(`Could not parse result from rust binary. 
              
              Ensure that the binary is printing the result in the following format:
              println!("Finished Local Invocation: {}", response_string);
              
              `)
            }
            result = JSON.parse(unparsedResult!)
          } else if (path.extname(pathofFileRelativeToCli) === '.py') {
            const pathOfLambdaContext = path.resolve(
              process.env.VENDIA_REPO_ROOT!,
              'src/share/share_backend/share_backend/deployer/lambda_context.py',
            )
            const lambda_context = await python(pathOfLambdaContext)
            const pyModule = await python(pathofFileRelativeToCli)
            const json = await python('json')
            const os = await python('os')
            await os.environ.update(env)
            const context = await lambda_context.LambdaContext(
              payload.context.awsRequestId,
              payload.context.clientContext,
              {},
              30 * 1000 + Date.now(), // Epoch + 30 seconds
              payload.context.invokedFunctionArn,
            )
            const pythonResult = await pyModule.lambda_handler(payload.event, context)
            result = JSON.parse(await json.dumps(pythonResult))
          } else if (path.extname(pathofFileRelativeToCli) === '.js') {
            result = await invokeByPath(pathofFileRelativeToCli, payload.event, payload.context)
          } else {
            consola.warn("File extension not supported. Must be '.js', '.py', or a rust binary")
          }

          consola.info(JSON.stringify(result, null, 2))

          const publishedResult = {
            context: payload.context,
            result,
          }
          await mqttClient.publish({
            qos: mqtt5.QoS.AtLeastOnce,
            topicName: topic,
            payload: JSON.stringify(publishedResult),
          })
          consola.box(`********** Sent Response (${payload.context.awsRequestId}) **********`)
        }
      }
    } catch (e) {
      consola.error(e)
      // Swallow the error so that the proxy doesn't crash
    }
  })
  mqttClient.on('error', async (e) => {
    consola.error(e)
  })

  const exitContext = {
    functionName,
    region,
    runtime: lambdaConfig.Runtime!,
    tmpDir: fullyResolvedTmpDir,
  }

  setTimeout(() => {
    consola.warn('45 minute timeout reached. Exiting...')
    exitHandler({ event: 'timeout', context: exitContext })
  }, 45 * 60 * 1000) // 45 minutes - attempting to keep this shorter than the 1 hour timeout for awsume

  process.on('exit', exitHandler.bind(null, { event: 'exit', context: exitContext }))
  process.on('SIGINT', exitHandler.bind(null, { event: 'SIGINT', context: exitContext }))
  process.on('SIGUSR1', exitHandler.bind(null, { event: 'SIGUSR1', context: exitContext }))
  process.on('SIGUSR2', exitHandler.bind(null, { event: 'SIGUSR2', context: exitContext }))
  process.on('uncaughtException', (...args) => {
    consola.error(args)
    exitHandler({ event: 'uncaughtException', context: exitContext })
  })
}

export const handler = async (argv: Args) => {
  let { lambda: functionName, file: localPath, plane, uni: uniName, node: nodeName } = argv

  const { consola } = await import('consola')

  let region = 'us-east-1'
  if (!plane) {
    plane = (await consola.prompt('Select the context (eg. control plane) in which to deploy the proxy:', {
      type: 'select',
      options: ['control', 'data'],
      default: 'control',
    })) as 'control' | 'data'
  }

  while (!localPath || !fs.existsSync(path.resolve(process.cwd(), localPath))) {
    localPath = (await consola.prompt(
      'Please provide the path to the local file relative to the current working directory',
    )) as string

    if (!fs.existsSync(path.resolve(process.cwd(), localPath))) {
      consola.error('File does not exist')
    }
  }

  if (plane === 'control') {
    if (!functionName) {
      functionName = (await consola.prompt('Please provide the full function name (eg. VendiaShareDeployer)')) as string
    }
    consola.box(`mapi dev proxy --plane control --lambda ${functionName} --file ${localPath}`)
  }

  if (plane === 'data') {
    if (!uniName) {
      uniName = (await consola.prompt(
        'Please provide a fully qualified Uni name (eg. test-uni.unis.vendia.com)',
      )) as string
    }
    const uni = await fetchUniByName(uniName)

    if (!nodeName) {
      nodeName = await consola.prompt('Select a Node:', {
        type: 'select',
        options: uni.nodes.map((n) => n.name),
        default: uni.nodes[0].name,
      })
    }
    const node = uni.nodes.find((n) => n.name === nodeName)

    region = node?.region!

    if (!functionName) {
      functionName = (await consola.prompt(
        'Please provide the function alias for the data plane operation (eg. graphql)',
      )) as string
    }

    consola.box(
      `mapi dev proxy --plane data --uni ${uniName} --node ${nodeName} --lambda ${functionName} --file ${localPath}`,
    )

    // If the uni uses the standard base namespace of `unis.venida.net (or .com)`, then it is not included in as part
    // of the lambda function name. If it is a custom namespace (ex: ledgerfoundry.com) then it does need to be included
    // ex: qgraphql_test-local-dev_unis_jakepartusch_com_Vendia
    let uniNameInFunctionName = uniName
    if (uniName.endsWith('unis.vendia.net')) {
      // remove the `unis.vendia.net` from the end of the function name
      uniNameInFunctionName = uniName.replace('.unis.vendia.net', '')
    }

    functionName = `${functionName}_${uniNameInFunctionName.replace(/\./g, '_')}_${node?.name}`
  }

  const topic = `local/${functionName}`

  await run({
    region,
    functionName: functionName!,
    localPath,
    topic,
    skipDeploy: argv.skipDeploy,
  })
}
