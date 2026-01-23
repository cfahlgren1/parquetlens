import { streamParquet } from "../src/index.js";

for await (const row of streamParquet(process.argv[2]!)) {
  console.log(row);
}
