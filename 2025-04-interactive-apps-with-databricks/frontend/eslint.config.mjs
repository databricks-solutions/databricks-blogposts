import { dirname } from "path";
import { fileURLToPath } from "url";
import { FlatCompat } from "@eslint/eslintrc";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const compat = new FlatCompat({
  baseDirectory: __dirname,
});

const eslintConfig = [
  ...compat.config({
    extends: ['next'],
    rules: {
      // Disable React entity escaping rule
      'react/no-unescaped-entities': 'off',
      
      // Disable TypeScript unused var checks
      '@typescript-eslint/no-unused-vars': 'on',
      
      // Disable TypeScript explicit any checks
      '@typescript-eslint/no-explicit-any': 'off'
    },
  }),
]

export default eslintConfig