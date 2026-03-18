export function getVisibilityAwareRefetchInterval(activeMs = 10_000, hiddenMs = 60_000) {
  if (typeof document === "undefined") return activeMs;
  return document.visibilityState === "visible" ? activeMs : hiddenMs;
}
