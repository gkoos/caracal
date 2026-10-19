/** AWS SDK v3 adapter entry point. */

export type {
  AwsSdkAdapterOptions,
  AwsSdkClient,
  AwsSdkReplay,
} from "./adapters/aws-sdk/index.js"
export {
  awsSdkAdapter,
  awsSdkErrorClassification,
} from "./adapters/aws-sdk/index.js"
