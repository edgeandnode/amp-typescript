/**
 * Implementation adapted from https://github.com/crouchcd/pkce-challenge.
 *
 * MIT License
 *
 * Copyright (c) 2019
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
import * as Effect from "effect/Effect"

/**
 * Creates an array of length `size` of random bytes (0 to 255).
 */
const getRandomValues = (size: number) => crypto.getRandomValues(new Uint8Array(size))

/**
 * Generate cryptographically strong random string of the specified `size`.
 */
const random = (size: number) => {
  const mask = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~"
  /// Ensure an even distribution of randomly selected values from the mask
  const evenDistCutoff = Math.pow(2, 8) - Math.pow(2, 8) % mask.length

  let result = ""
  while (result.length < size) {
    const randomBytes = getRandomValues(size - result.length)

    for (const randomByte of randomBytes) {
      if (randomByte < evenDistCutoff) {
        result += mask[randomByte % mask.length]
      }
    }
  }

  return result
}

/**
 * Generate a PKCE challenge verifier of `length` characters.
 */
const generateVerifier = (length: number): string => random(length)

/**
 * Generate a PKCE code challenge from a code verifier.
 */
export const generateChallenge = async (code_verifier: string) => {
  const buffer = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(code_verifier)
  )
  // Generate base64url string. `btoa` is deprecated in Node.js but is used here
  // for web browser compatibility (which has no good replacement yet, see also
  // https://github.com/whatwg/html/issues/6811)
  return btoa(String.fromCharCode(...new Uint8Array(buffer)))
    .replace(/\//g, "_")
    .replace(/\+/g, "-")
    .replace(/=/g, "")
}

/**
 * Generate a PKCE challenge pair of the specified `length`.
 *
 * The `length` must be in the range of 43-128 (defaults to 43).
 */
export const pkceChallenge = Effect.fnUntraced(function*(length?: number) {
  if (!length) {
    length = 43
  }

  if (length < 43 || length > 128) {
    return yield* Effect.dieMessage(
      `Expected a length between 43 and 128. Received ${length}.`
    )
  }

  const verifier = generateVerifier(length)
  const challenge = yield* Effect.promise(() => generateChallenge(verifier))

  return {
    codeVerifier: verifier,
    codeChallenge: challenge
  }
})
