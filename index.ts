import { DescribeEndpointCommand, IoTClient } from "@aws-sdk/client-iot";
import {
  GetFunctionConfigurationCommand,
  GetFunctionConfigurationCommandOutput,
  LambdaClient,
  UpdateFunctionCodeCommand,
  UpdateFunctionConfigurationCommand,
  UpdateFunctionConfigurationCommandInput,
} from "@aws-sdk/client-lambda";
import { toUtf8 } from "@aws-sdk/util-utf8-browser";
import * as archiver from "archiver";
import { iot, mqtt5 } from "aws-iot-device-sdk-v2";
import { exec } from "child_process";
import * as crypto from "crypto";
import * as esbuild from "esbuild";
import { once } from "events";
import * as fs from "fs";
import * as path from "path";
import { Argv } from "yargs";
import * as os from "os";

interface Args {
  name?: string;
  path?: string;
  skipDeploy?: boolean;
}

export const builder = (yargs: Argv<Args>) => {
  return;
};

const invokeByPath = async (path: string, event: any, context: any) => {
  const { handler: fn } = await import(
    `${path}?cacheBust=${Number(new Date())}`
  );
  console.log(handler);
  try {
    const response = await fn(event, context);
    return response;
  } catch (e) {
    throw e;
  } finally {
    for (const path in require.cache) {
      if (path.endsWith(".js")) {
        // only clear *.js, not *.node
        delete require.cache[path];
      }
    }
  }
};

const getIotWebsocketEndpoint = async (region: string) => {
  const iot = new IoTClient({
    region,
  });
  const params = {
    endpointType: "iot:Data-ATS",
  };
  return iot.send(new DescribeEndpointCommand(params));
};

const creatClientConfig = ({
  websocketEndpoint,
  region,
}: {
  websocketEndpoint: string;
  region: string;
}) => {
  const builder =
    iot.AwsIotMqtt5ClientConfigBuilder.newWebsocketMqttBuilderWithSigv4Auth(
      websocketEndpoint,
      {
        region,
      }
    );
  builder.withConnectProperties({
    keepAliveIntervalSeconds: 1200,
  });
  return builder.build();
};

const createMqttClient = ({
  websocketEndpoint,
  region,
}: {
  websocketEndpoint: string;
  region: string;
}): mqtt5.Mqtt5Client => {
  const config: mqtt5.Mqtt5ClientConfig = creatClientConfig({
    websocketEndpoint,
    region,
  });
  return new mqtt5.Mqtt5Client(config);
};

const updateLambdaFunctionCode = async (
  functionName: string,
  zipFile: Buffer,
  region: string
) => {
  const lambdaClient = new LambdaClient({ region });
  const params = {
    FunctionName: functionName,
    ZipFile: zipFile,
  };
  const command = new UpdateFunctionCodeCommand(params);
  return lambdaClient.send(command);
};

const updateLambdaFunctionConfiguration = async ({
  functionName,
  region,
  runtime,
  handler,
}: {
  functionName: string;
  region: string;
  runtime: string;
  handler: string;
}) => {
  const lambdaClient = new LambdaClient({ region });
  const params: UpdateFunctionConfigurationCommandInput = {
    // @ts-ignore
    Runtime: runtime,
    Handler: handler,
    FunctionName: functionName,
  };
  const command = new UpdateFunctionConfigurationCommand(params);
  return lambdaClient.send(command);
};

const getLambdaConfiguration = async (functionName: string, region: string) => {
  const lambdaClient = new LambdaClient({ region });
  const params = {
    FunctionName: functionName,
  };
  const command = new GetFunctionConfigurationCommand(params);
  return lambdaClient.send(command);
};

const copyDirectorySync = (source: string, target: string) => {
  fs.readdirSync(source).forEach((file) => {
    const sourceFile = path.join(source, file);
    const targetFile = path.join(target, file);

    if (fs.lstatSync(sourceFile).isDirectory()) {
      if (!fs.existsSync(targetFile)) {
        fs.mkdirSync(targetFile);
      }
      copyDirectorySync(sourceFile, targetFile);
    } else {
      fs.copyFileSync(sourceFile, targetFile);
    }
  });
};

const zipDirectory = (source: string, out: string) => {
  const archive = archiver.default("zip", { zlib: { level: 9 } });
  const stream = fs.createWriteStream(out);

  return new Promise((resolve, reject) => {
    archive
      .directory(source, false)
      .on("error", (err: any) => reject(err))
      .pipe(stream);

    stream.on("close", () => resolve(""));
    archive.finalize();
  });
};

const hashDirectory = (directory: string) => {
  const hash = crypto.createHash("sha256");
  const files = fs.readdirSync(directory);

  for (const file of files) {
    const filePath = path.join(directory, file);
    const stat = fs.statSync(filePath);

    if (stat.isFile()) {
      const fileData = fs.readFileSync(filePath);
      hash.update(fileData);
    } else if (stat.isDirectory()) {
      const nestedHash = hashDirectory(filePath);
      hash.update(nestedHash);
    }
  }

  return hash.digest("hex");
};

const runNpmInstall = async (directory: string): Promise<void> => {
  const { consola } = await import("consola");
  return new Promise((resolve, reject) => {
    exec("npm install", { cwd: directory }, (error, stdout, stderr) => {
      if (error) {
        consola.fatal(`Error running npm install: ${error}`);
        reject(error);
        return;
      }
      consola.debug(`npm install output: ${stdout}`);
      resolve();
    });
  });
};

const buildProxyHandler = async (
  handlerName: string,
  tmpDir: string
): Promise<string> => {
  await esbuild.build({
    entryPoints: [path.resolve(__dirname, `./local_proxy/iot-publish.ts`)],
    outfile: path.resolve(tmpDir, `dist/${handlerName?.split(".")?.[0]}.js`),
    bundle: true,
    platform: "node",
    target: "node20",
    external: ["aws-crt"],
  });
  copyDirectorySync(
    path.resolve(__dirname, `./local_proxy`),
    path.resolve(tmpDir, "dist")
  );
  await runNpmInstall(path.resolve(tmpDir, "dist"));
  const hash = hashDirectory(path.resolve(tmpDir, "dist"));
  const zipName = `asset.${hash}.zip`;
  await zipDirectory(
    path.resolve(tmpDir, `dist`),
    path.resolve(tmpDir, zipName)
  );
  return zipName;
};

const exitHandler = async ({
  event,
  context,
}: {
  event: string;
  context: {
    functionName: string;
    region: string;
    runtime: string;
    tmpDir: string;
  };
}) => {
  const { consola } = await import("consola");
  const { functionName, region, tmpDir, runtime } = context;
  consola.warn(`Received ${event} event. Cleaning up...`);
  if (runtime !== "nodejs20.x") {
    const filePath = path.resolve(tmpDir, "backup", `${functionName}.json`);
    const lambdaConfiguration: GetFunctionConfigurationCommandOutput =
      JSON.parse(
        fs.readFileSync(filePath).toString()
      ) as GetFunctionConfigurationCommandOutput;
    await updateLambdaFunctionConfiguration({
      functionName,
      region,
      runtime: lambdaConfiguration.Runtime!,
      handler: lambdaConfiguration.Handler!,
    });
  }
  fs.rmSync(tmpDir, { recursive: true });
  process.exit(0);
};

// Use the cli argument to invoke the local function
const run = async ({
  region,
  functionName,
  localPath,
  topic,
  skipDeploy,
}: {
  region: string;
  functionName: string;
  localPath: string;
  topic: string;
  skipDeploy?: boolean;
}) => {
  // TODO: we should be able to import this normally, but commonjs seems to be broken in v3. We should fix this if they patch it.
  const { consola } = await import("consola");
  consola.start("Deploying local proxy...");

  const tmpdir = fs.mkdtempSync("tmp-proxy-");
  const fullyResolvedTmpDir = path.resolve(os.tmpdir(), tmpdir);

  consola.info("Fetching lambda configuration for", functionName);
  let lambdaConfig = await getLambdaConfiguration(functionName, region);
  if (lambdaConfig.Runtime !== "nodejs20.x") {
    consola.info("Lambda function is not using nodejs20.x runtime");
    consola.info("Backing up lambda function configuration...");
    fs.mkdirSync(path.resolve(fullyResolvedTmpDir, "backup"), {
      recursive: true,
    });
    const backupFile = path.resolve(
      fullyResolvedTmpDir,
      `backup/${functionName}.json`
    );
    fs.writeFileSync(backupFile, JSON.stringify(lambdaConfig, null, 2));

    consola.info("Updating lambda function configuration to nodejs20.x");
    await updateLambdaFunctionConfiguration({
      functionName,
      region,
      runtime: "nodejs20.x",
      handler: "index.handler",
    });
    lambdaConfig = await getLambdaConfiguration(functionName, region);
  }
  const handlerName = lambdaConfig.Handler!;

  consola.info("Building local proxy with handler", handlerName);
  const zipName = await buildProxyHandler(handlerName, fullyResolvedTmpDir);

  consola.info("Swapping lambda function code with proxy...");
  if (!skipDeploy) {
    await updateLambdaFunctionCode(
      functionName,
      fs.readFileSync(path.resolve(fullyResolvedTmpDir, zipName)),
      region
    );
  }

  const env = lambdaConfig.Environment?.Variables!;
  if (env) {
    env["AWS_DEFAULT_REGION"] = region;
    for (const key in env) {
      process.env[key] = env?.[key];
    }
    consola.info("Lambda environment variables applied locally.");
  }

  //get the path relative to the directory that the cli is run from
  const pathofFileRelativeToCli = path.join(process.cwd(), localPath);

  consola.info("Connecting to websocket endpoint...");
  const websocketEndpoint = await getIotWebsocketEndpoint(region);
  if (!websocketEndpoint.endpointAddress) {
    throw new Error("No websocket endpoint found");
  }

  const mqttClient: mqtt5.Mqtt5Client = createMqttClient({
    websocketEndpoint: websocketEndpoint.endpointAddress!,
    region,
  });

  const connectionSuccess = once(mqttClient, "connectionSuccess");

  mqttClient.start();

  await connectionSuccess;
  consola.success("Connected. Ready for requests...");

  await mqttClient.subscribe({
    subscriptions: [{ qos: mqtt5.QoS.AtLeastOnce, topicFilter: topic }],
  });

  mqttClient.on(
    "messageReceived",
    async (eventData: mqtt5.MessageReceivedEvent) => {
      try {
        if (eventData.message.payload) {
          const payload = JSON.parse(
            toUtf8(new Uint8Array(eventData.message.payload as ArrayBuffer))
          );
          if (payload.event) {
            consola.box(
              `********** Received Event (${payload.context.awsRequestId}) **********`
            );
            let result;
            if (pathofFileRelativeToCli.endsWith(".js")) {
              result = await invokeByPath(
                pathofFileRelativeToCli,
                payload.event,
                payload.context
              );
            } else {
              consola.warn("File extension not supported. Must be '.js'");
            }

            consola.info(JSON.stringify(result, null, 2));

            const publishedResult = {
              context: payload.context,
              result,
            };
            await mqttClient.publish({
              qos: mqtt5.QoS.AtLeastOnce,
              topicName: topic,
              payload: JSON.stringify(publishedResult),
            });
            consola.box(
              `********** Sent Response (${payload.context.awsRequestId}) **********`
            );
          }
        }
      } catch (e) {
        consola.error(e);
        // Swallow the error so that the proxy doesn't crash
      }
    }
  );
  mqttClient.on("error", async (e) => {
    consola.error(e);
  });

  const exitContext = {
    functionName,
    region,
    runtime: lambdaConfig.Runtime!,
    tmpDir: fullyResolvedTmpDir,
  };

  setTimeout(() => {
    consola.warn("45 minute timeout reached. Exiting...");
    exitHandler({ event: "timeout", context: exitContext });
  }, 45 * 60 * 1000); // 45 minutes - attempting to keep this shorter than the 1 hour timeout for awsume

  process.on(
    "exit",
    exitHandler.bind(null, { event: "exit", context: exitContext })
  );
  process.on(
    "SIGINT",
    exitHandler.bind(null, { event: "SIGINT", context: exitContext })
  );
  process.on(
    "SIGUSR1",
    exitHandler.bind(null, { event: "SIGUSR1", context: exitContext })
  );
  process.on(
    "SIGUSR2",
    exitHandler.bind(null, { event: "SIGUSR2", context: exitContext })
  );
  process.on("uncaughtException", (...args) => {
    consola.error(args);
    exitHandler({ event: "uncaughtException", context: exitContext });
  });
};

export const handler = async (argv: Args) => {
  let { name: functionName, path: localPath } = argv;

  const { consola } = await import("consola");

  let region = "us-east-1";

  while (!localPath || !fs.existsSync(path.resolve(process.cwd(), localPath))) {
    localPath = (await consola.prompt(
      "Please provide the path to the local file relative to the current working directory"
    )) as string;

    if (!fs.existsSync(path.resolve(process.cwd(), localPath))) {
      consola.error("File does not exist");
    }
  }

  const topic = `local/${functionName}`;

  await run({
    region,
    functionName: functionName!,
    localPath,
    topic,
    skipDeploy: argv.skipDeploy,
  });
};
