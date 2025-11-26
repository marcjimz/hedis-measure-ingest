/**
 * API Configuration
 *
 * Provides the base URL for backend API calls.
 * Uses NEXT_PUBLIC_API_URL environment variable set during deployment.
 */

/**
 * Get the backend API base URL
 */
export function getApiUrl(): string {
  // Use environment variable if set (Databricks Apps deployment)
  if (process.env.NEXT_PUBLIC_API_URL) {
    return process.env.NEXT_PUBLIC_API_URL;
  }

  // Fallback for local development
  if (typeof window === 'undefined') {
    // Server-side
    return 'http://localhost:8000';
  }

  // Client-side - use relative path for local dev
  return 'http://localhost:8000';
}

/**
 * Construct full API URL for an endpoint
 * @param endpoint - API endpoint path (e.g., '/api/chat')
 * @returns Full URL to the API endpoint
 */
export function apiUrl(endpoint: string): string {
  const baseUrl = getApiUrl();
  // Remove leading slash from endpoint if present
  const path = endpoint.startsWith('/') ? endpoint.slice(1) : endpoint;
  return `${baseUrl}/${path}`;
}
