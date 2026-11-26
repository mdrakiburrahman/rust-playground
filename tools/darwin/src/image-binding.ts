export function parityImageIds(log: string, resultImages: unknown): Record<string, string> {
  const prefix = "[oat] immutable local images: ";
  const lines = log.split("\n").filter(line => line.startsWith(prefix));
  if (lines.length !== 1) throw new Error("expected exactly one immutable image admission record");
  const images: unknown = JSON.parse(lines[0]!.slice(prefix.length));
  if (typeof images !== "object" || images === null || Array.isArray(images)) {
    throw new Error("invalid immutable image admission record");
  }
  const roles = ["catalog", "client", "collector", "maintenance", "router", "sqlserver"];
  if (Object.keys(images).sort().join(",") !== roles.join(",")) throw new Error("invalid immutable image admission record");
  const pinned: Record<string, string> = {};
  for (const role of roles) {
    const image = (images as Record<string, unknown>)[role];
    if (typeof image !== "string" || !/^sha256:[a-f0-9]{64}$/.test(image)) throw new Error("invalid immutable image admission record");
    pinned[role] = image;
  }
  if (!Array.isArray(resultImages) || resultImages.length !== 4
    || !["client", "collector", "maintenance", "router"].every(role => resultImages.includes(`${role}:${pinned[role]}`))) {
    throw new Error("runtime image evidence differs from immutable admission");
  }
  return pinned;
}
