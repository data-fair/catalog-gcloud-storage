import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    environment: 'node',
    globals: true,
    include: ['test-it/*.test.ts'],
    coverage: {
      reporter: ['text', 'html'],
    },
  },
})
