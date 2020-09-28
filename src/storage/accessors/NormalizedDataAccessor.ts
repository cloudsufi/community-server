import type { Readable } from 'stream';
import type { Patch } from '../../ldp/http/Patch';
import type { Representation } from '../../ldp/representation/Representation';
import type { RepresentationMetadata } from '../../ldp/representation/RepresentationMetadata';
import type { ResourceIdentifier } from '../../ldp/representation/ResourceIdentifier';
import { NotFoundHttpError } from '../../util/errors/NotFoundHttpError';
import { ensureTrailingSlash, trimTrailingSlashes } from '../../util/Util';
import type { DataAccessor } from './DataAccessor';

/**
 * Abstract class that implements `getNormalizedMetadata` by calling the `getMetadata` with the incoming identifier
 * and trying again by changing the trailing slash and trying again.
 */
export abstract class NormalizedDataAccessor implements DataAccessor {
  public async getNormalizedMetadata(identifier: ResourceIdentifier): Promise<RepresentationMetadata> {
    const hasSlash = identifier.path.endsWith('/');
    try {
      return await this.getMetadata(identifier);
    } catch (error: unknown) {
      if (error instanceof NotFoundHttpError) {
        return this.getMetadata(
          { path: hasSlash ? trimTrailingSlashes(identifier.path) : ensureTrailingSlash(identifier.path) },
        );
      }
      throw error;
    }
  }

  public abstract async canHandle(representation: Representation): Promise<void>;

  public abstract async deleteResource(identifier: ResourceIdentifier): Promise<void>;

  public abstract async getData(identifier: ResourceIdentifier): Promise<Readable>;

  public abstract async getMetadata(identifier: ResourceIdentifier): Promise<RepresentationMetadata>;

  public abstract async modifyResource(identifier: ResourceIdentifier, patch: Patch): Promise<void>;

  public abstract async writeContainer(identifier: ResourceIdentifier, metadata?: RepresentationMetadata):
  Promise<void>;

  public abstract async writeDataResource(identifier: ResourceIdentifier, data: Readable,
    metadata?: RepresentationMetadata): Promise<void>;
}
