import { streamParquet } from "../src/index.js";

const url =
  process.argv[2] ??
  "https://huggingface.co/datasets/open-llm-leaderboard/contents/resolve/main/data/train-00000-of-00001.parquet";

let count = 0;
for await (const row of streamParquet(url)) {
  console.log(row);
  if (++count >= 5) break;
}
