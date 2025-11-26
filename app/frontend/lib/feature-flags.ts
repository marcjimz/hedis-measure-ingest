// Feature flags configuration for progressive rollout
export const featureFlags = {
  chats: true,
  measures: true,
  patients: false, // Next release
  reviews: false, // Final release
} as const

export type FeatureFlag = keyof typeof featureFlags

export function isFeatureEnabled(feature: FeatureFlag): boolean {
  return featureFlags[feature]
}
