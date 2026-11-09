export function kubectlFailureDetail(args: readonly string[], redact: (text: string) => string,
  secrets: readonly string[]): (stderr: string) => string {
  return stderr => {
    let safe = stderr;
    for (const secret of [...secrets].filter(Boolean).sort((a, b) => b.length - a.length))
      safe = safe.replaceAll(secret, '******');
    safe = safe.replace(/\b(?:bearer|basic)\s+\S+/gi, '******')
      .replace(/https?:\/\/[^\s"'<>]+/gi, '[endpoint]')
      .replace(/((?:token|password|secret|authorization|sig)\s*[=:]\s*)\S+/gi, '$1******');
    // Only the operation is useful context; exec argv may contain private data.
    const operation = ['get', 'exec', 'cp', 'delete'].find(value => args.includes(value)) ?? 'command';
    return `Retained cleanup ${operation}: ${redact(safe).replace(/[\x00-\x08\x0b-\x1f\x7f]/g, '').trim()}`.slice(0, 2000);
  };
}
