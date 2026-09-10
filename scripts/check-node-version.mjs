const minimumMajor = 20
const currentMajor = Number(process.versions.node.split(".")[0])

if (!Number.isInteger(currentMajor) || currentMajor < minimumMajor) {
  throw new Error(
    `Caracal requires Node.js ${minimumMajor} or newer; found ${process.version}`,
  )
}

console.log(
  `Node.js ${process.version} satisfies Caracal's >=${minimumMajor} requirement.`,
)
