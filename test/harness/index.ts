/** Runner-agnostic adapter contract checks; source remains outside src/. */

export type {
  AdapterContractAbortCase,
  AdapterContractCapabilityCase,
  AdapterContractCheck,
  AdapterContractClassificationCase,
  AdapterContractOptions,
  AdapterContractSuccess,
  AdapterContractSuite,
} from "./adapter-contract.js"
export {
  defineAdapterContractSuite,
  runAdapterContractSuite,
} from "./adapter-contract.js"
