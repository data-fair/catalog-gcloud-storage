/**
 * Parse the service account key given by the user.
 *
 * Only `service_account` keys are accepted: google-auth-library also accepts other credential
 * configurations (`external_account`, `impersonated_service_account`, etc.) that can read local
 * files, fetch arbitrary URLs or run commands and send the result to a token URL of their choice.
 * Any other type (or a missing type) would be used as is by the library.
 *
 * @param serviceAccount - The JSON string of the service account key.
 * @returns The parsed service account key.
 * @throws If the JSON is invalid or is not a service account key.
 */
export const parseServiceAccount = (serviceAccount: string): Record<string, any> => {
  const credentials = JSON.parse(serviceAccount)
  if (credentials?.type !== 'service_account') {
    throw new Error('Only service account keys ("type": "service_account") are supported')
  }
  return credentials
}
