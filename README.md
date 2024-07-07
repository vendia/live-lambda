# live-lambda

A powerful development CLI tool designed for AWS Lambda functions. Proxy your lambda function executions directly to your development environment to enable quicker iterations and hot reloading.

## Overview

![Live Lambda](./live-lambda.png)

## Features

- **Easy Integration**: Quickly integrate with existing AWS Lambda functions.
- **Performance Optimization**: Designed to minimize latency and maximize performance.
- **Security**: Implements best practices to ensure secure communication between your services and AWS Lambda.
- **Customization**: Offers extensive customization options to fit various use cases and requirements.

## Installation

Install `live-lambda` globally via npm:

```bash
npm install -g @vendia/live-lambda
```

Or, add it to your project as a development dependency:

```bash
npm install --save-dev @vendia/live-lambda
```

## Usage

After installation, you can use live-lambda in your command line:

```bash
live-lambda [options]
```

## Options

-n, --name: The name of the lambda function to proxy.
-p, --path: The path to the local file to proxy to.
--skip-deploy: Skip deployment of the proxy and restart the websocket listener

-v, --version: Display the version number.
-h, --help: Show help and usage information.

## Examples

Here's a simple example to get you started:

```bash
live-lambda start --name HelloWorldFunction --path lambda/index.js
```

This command does something amazing with your AWS Lambda functions.

## Contributing

We welcome contributions! Please read our Contributing Guide for details on how to submit pull requests, how to propose features, and how to report bugs.

### License

live-lambda is licensed under the ISC license. See the LICENSE file for more details.
