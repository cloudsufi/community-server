import { Authorizer } from './Authorizer';

/**
 * Authorizer which allows all access independent of the identifier and requested permissions.
 */
export class AllowEverythingAuthorizer extends Authorizer {
  public async canHandle(): Promise<void> {
    // Can handle all requests
  }

  public async handle(): Promise<void> {
    // Allows all actions
  }
}
