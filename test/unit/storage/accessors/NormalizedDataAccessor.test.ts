import { RepresentationMetadata } from '../../../../src/ldp/representation/RepresentationMetadata';
import type { ResourceIdentifier } from '../../../../src/ldp/representation/ResourceIdentifier';
import { NormalizedDataAccessor } from '../../../../src/storage/accessors/NormalizedDataAccessor';
import { NotFoundHttpError } from '../../../../src/util/errors/NotFoundHttpError';

class SimpleDataAccessor extends NormalizedDataAccessor {
  public isFile = false;
  public isFolder = false;

  public async getMetadata(identifier: ResourceIdentifier): Promise<RepresentationMetadata> {
    if (identifier.path === 'error') {
      throw new Error('artificial error');
    }
    if (identifier.path.endsWith('/') && this.isFolder) {
      return new RepresentationMetadata(identifier.path);
    }
    if (!identifier.path.endsWith('/') && this.isFile) {
      return new RepresentationMetadata(identifier.path);
    }
    throw new NotFoundHttpError();
  }

  public async canHandle(): Promise<void> {
    // Dummy
  }

  public async deleteResource(): Promise<void> {
    // Dummy
  }

  public async getData(): Promise<any> {
    // Dummy
  }

  public async modifyResource(): Promise<void> {
    // Dummy
  }

  public async writeContainer(): Promise<void> {
    // Dummy
  }

  public async writeDataResource(): Promise<void> {
    // Dummy
  }
}

describe('A NormalizedDataAccessor', (): void => {
  let accessor: SimpleDataAccessor;

  beforeEach(async(): Promise<void> => {
    accessor = new SimpleDataAccessor();
  });

  it('returns the corresponding metadata if it has an exact match.', async(): Promise<void> => {
    accessor.isFile = true;
    let metadata = await accessor.getNormalizedMetadata({ path: 'path' });
    expect(metadata.identifier.value).toBe('path');

    accessor.isFile = false;
    accessor.isFolder = true;
    metadata = await accessor.getNormalizedMetadata({ path: 'path/' });
    expect(metadata.identifier.value).toBe('path/');
  });

  it('returns the metadata with different slash if it exists.', async(): Promise<void> => {
    accessor.isFile = true;
    let metadata = await accessor.getNormalizedMetadata({ path: 'path/' });
    expect(metadata.identifier.value).toBe('path');

    accessor.isFile = false;
    accessor.isFolder = true;
    metadata = await accessor.getNormalizedMetadata({ path: 'path' });
    expect(metadata.identifier.value).toBe('path/');
  });

  it('will throw the corresponding error if no matches exist.', async(): Promise<void> => {
    await expect(accessor.getNormalizedMetadata({ path: 'path' })).rejects.toThrow(NotFoundHttpError);
  });

  it('will throw any non-404 error that gets thrown.', async(): Promise<void> => {
    await expect(accessor.getNormalizedMetadata({ path: 'error' })).rejects.toThrow(new Error('artificial error'));
  });
});
