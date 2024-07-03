import { DescribeEndpointCommand, IoTClient } from '@aws-sdk/client-iot'
import { toUtf8 } from '@aws-sdk/util-utf8-browser'
//@ts-ignore
import { iot, mqtt5 } from 'aws-iot-device-sdk-v2'
import { once } from 'events'

const { AWS_LAMBDA_FUNCTION_NAME } = process.env

const getIotWebsocketEndpoint = async () => {
  const iot = new IoTClient({})
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

function createClient({ websocketEndpoint, region }: { websocketEndpoint: string; region: string }): mqtt5.Mqtt5Client {
  const config: mqtt5.Mqtt5ClientConfig = creatClientConfig({
    websocketEndpoint,
    region,
  })

  return new mqtt5.Mqtt5Client(config)
}

const handler = async (event: any, context: any) => {
  const websocketEndpoint = await getIotWebsocketEndpoint()

  const client: mqtt5.Mqtt5Client = createClient({
    websocketEndpoint: websocketEndpoint.endpointAddress as string,
    region: websocketEndpoint.endpointAddress?.split('.')[2] as string,
  })

  const connectionSuccess = once(client, 'connectionSuccess')

  client.start()

  await connectionSuccess

  await client.subscribe({
    subscriptions: [{ qos: mqtt5.QoS.AtLeastOnce, topicFilter: `local/${AWS_LAMBDA_FUNCTION_NAME}` }],
  })

  const resultPromise = new Promise((resolve, reject) => {
    client.on('messageReceived', async (eventData: mqtt5.MessageReceivedEvent) => {
      if (eventData.message.payload) {
        const payload = JSON.parse(toUtf8(new Uint8Array(eventData.message.payload as ArrayBuffer)))
        if (payload.result && payload.context.awsRequestId === context.awsRequestId) {
          resolve(payload.result)
        }
      }
    })
  })

  client.publish({
    qos: mqtt5.QoS.AtLeastOnce,
    topicName: `local/${AWS_LAMBDA_FUNCTION_NAME}`,
    payload: JSON.stringify({ event, context }),
  })

  return resultPromise
}

module.exports = { handler }
