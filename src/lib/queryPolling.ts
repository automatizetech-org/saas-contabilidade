export function getVisibilityAwareRefetchInterval(activeMs = 30_000, hiddenMs = 120_000) {
  if (typeof document === "undefined") return activeMs;
  return document.visibilityState === "visible" ? activeMs : hiddenMs;
}
