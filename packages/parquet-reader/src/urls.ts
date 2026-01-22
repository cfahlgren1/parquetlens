export type ResolvedParquetUrl = {
  url: string;
};

export function resolveParquetUrl(input: string): ResolvedParquetUrl | null {
  if (input.startsWith("hf://")) {
    return resolveHuggingFaceUrl(input);
  }

  if (input.startsWith("http://") || input.startsWith("https://")) {
    return { url: input };
  }

  return null;
}

function resolveHuggingFaceUrl(input: string): ResolvedParquetUrl {
  const match = input.match(/^hf:\/\/(datasets|models)\/([^@\/]+)\/([^@\/]+)(?:@([^\/]+))?\/(.+)$/);

  if (!match) {
    throw new Error(`Invalid hf:// URL: ${input}`);
  }

  const [, type, user, repo, branch = "main", filePath] = match;

  return {
    url: `https://huggingface.co/${type}/${user}/${repo}/resolve/${branch}/${filePath}`,
  };
}
