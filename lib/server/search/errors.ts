import { ZodError } from "zod";

export class ResourceNotFoundError extends Error {}

export class InputValidationError extends Error {
  constructor(private readonly error: ZodError) {
    super("Invalid search filters.");
    this.name = "InputValidationError";
  }

  flatten() {
    return this.error.flatten();
  }
}

export function isInputValidationError(
  error: unknown,
): error is InputValidationError {
  return error instanceof InputValidationError;
}
