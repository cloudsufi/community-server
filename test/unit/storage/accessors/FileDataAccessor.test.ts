import streamifyArray from 'streamify-array';
import type { Representation } from '../../../../src/ldp/representation/Representation';
import { RepresentationMetadata } from '../../../../src/ldp/representation/RepresentationMetadata';
import { FileDataAccessor } from '../../../../src/storage/accessors/FileDataAccessor';
import { ExtensionBasedMapper } from '../../../../src/storage/ExtensionBasedMapper';
import { ConflictHttpError } from '../../../../src/util/errors/ConflictHttpError';
import { NotFoundHttpError } from '../../../../src/util/errors/NotFoundHttpError';
import { UnsupportedMediaTypeHttpError } from '../../../../src/util/errors/UnsupportedMediaTypeHttpError';
import { MetadataController } from '../../../../src/util/MetadataController';
import { DCTERMS, LDP, POSIX, RDF, XSD } from '../../../../src/util/UriConstants';
import { toNamedNode, toTypedLiteral } from '../../../../src/util/UriUtil';
import { readableToString } from '../../../../src/util/Util';
import { mockFs } from '../../../util/Util';

jest.mock('fs');

const rootFilePath = 'uploads';
const now = new Date();
const cache = mockFs(rootFilePath, now);

describe('A FileDataAccessor', (): void => {
  const base = 'http://test.com/';
  let accessor: FileDataAccessor;

  beforeEach(async(): Promise<void> => {
    cache.data = {};
    accessor = new FileDataAccessor(
      new ExtensionBasedMapper(base, rootFilePath),
      new MetadataController(),
    );
  });

  it('can only handle binary data.', async(): Promise<void> => {
    await expect(accessor.canHandle({ binary: true } as Representation)).resolves.toBeUndefined();
    await expect(accessor.canHandle({ binary: false } as Representation)).rejects
      .toThrow(new UnsupportedMediaTypeHttpError('Only binary data is supported.'));
  });

  describe('getting data', (): void => {
    it('throws a 404 if the identifier does not start with the base.', async(): Promise<void> => {
      await expect(accessor.getData({ path: 'badpath' })).rejects.toThrow(NotFoundHttpError);
    });

    it('throws a 404 if the identifier does not match an existing file.', async(): Promise<void> => {
      await expect(accessor.getData({ path: `${base}resource` })).rejects.toThrow(NotFoundHttpError);
    });

    it('throws a 404 if the identifier matches a directory.', async(): Promise<void> => {
      cache.data = { resource: {}};
      await expect(accessor.getData({ path: `${base}resource` })).rejects.toThrow(NotFoundHttpError);
    });

    it('returns the corresponding data.', async(): Promise<void> => {
      cache.data = { resource: 'data' };
      const stream = await accessor.getData({ path: `${base}resource` });
      await expect(readableToString(stream)).resolves.toBe('data');
    });
  });

  describe('getting metadata', (): void => {
    it('throws a 404 if the identifier does not start with the base.', async(): Promise<void> => {
      await expect(accessor.getMetadata({ path: 'badpath' })).rejects.toThrow(NotFoundHttpError);
    });

    it('throws a 404 if the identifier does not match an existing file.', async(): Promise<void> => {
      await expect(accessor.getMetadata({ path: `${base}resource` })).rejects.toThrow(NotFoundHttpError);
    });

    it('throws a 404 if it matches something that is no file or directory.', async(): Promise<void> => {
      cache.data = { resource: 5 };
      await expect(accessor.getMetadata({ path: `${base}resource` })).rejects.toThrow(NotFoundHttpError);
    });

    it('throws an error if something else went wrong.', async(): Promise<void> => {
      cache.data = { container: 'apple' };
      await expect(accessor.getMetadata({ path: `${base}container/container2/resource` })).rejects.toThrow();
    });

    it('throws a 404 if the trailing slash does not match its type.', async(): Promise<void> => {
      cache.data = { resource: 'data' };
      await expect(accessor.getMetadata({ path: `${base}resource/` })).rejects.toThrow(NotFoundHttpError);
      cache.data = { container: {}};
      await expect(accessor.getMetadata({ path: `${base}container` })).rejects.toThrow(NotFoundHttpError);
    });

    it('generates the metadata for a resource.', async(): Promise<void> => {
      cache.data = { 'resource.ttl': 'data' };
      const metadata = await accessor.getMetadata({ path: `${base}resource.ttl` });
      expect(metadata.identifier.value).toBe(`${base}resource.ttl`);
      expect(metadata.contentType).toBe('text/turtle');
      expect(metadata.get(RDF.type)?.value).toBe(LDP.Resource);
      expect(metadata.get(POSIX.size)).toEqualRdfTerm(toTypedLiteral('data'.length, XSD.integer));
      expect(metadata.get(DCTERMS.modified)).toEqualRdfTerm(toTypedLiteral(now.toISOString(), XSD.dateTime));
      expect(metadata.get(POSIX.mtime)).toEqualRdfTerm(toTypedLiteral(Math.floor(now.getTime() / 1000), XSD.integer));
    });

    it('generates the metadata for a container and its non-meta children.', async(): Promise<void> => {
      cache.data = { container: { resource: 'data', 'resource.meta': 'metadata', notAFile: 5, container2: {}}};
      const metadata = await accessor.getMetadata({ path: `${base}container/` });
      expect(metadata.identifier.value).toBe(`${base}container/`);
      expect(metadata.getAll(RDF.type)).toEqualRdfTermArray(
        [ toNamedNode(LDP.Container), toNamedNode(LDP.BasicContainer), toNamedNode(LDP.Resource) ],
      );
      expect(metadata.get(POSIX.size)).toEqualRdfTerm(toTypedLiteral(0, XSD.integer));
      expect(metadata.get(DCTERMS.modified)).toEqualRdfTerm(toTypedLiteral(now.toISOString(), XSD.dateTime));
      expect(metadata.get(POSIX.mtime)).toEqualRdfTerm(toTypedLiteral(Math.floor(now.getTime() / 1000), XSD.integer));
      expect(metadata.getAll(LDP.contains)).toEqualRdfTermArray(
        [ toNamedNode(`${base}container/resource`), toNamedNode(`${base}container/container2/`) ],
      );

      const childQuads = metadata.quads().filter((quad): boolean =>
        quad.subject.value === `${base}container/resource`);
      const childMetadata = new RepresentationMetadata(`${base}container/resource`).addQuads(childQuads);
      expect(childMetadata.get(RDF.type)?.value).toBe(LDP.Resource);
      expect(childMetadata.get(POSIX.size)).toEqualRdfTerm(toTypedLiteral('data'.length, XSD.integer));
      expect(childMetadata.get(DCTERMS.modified)).toEqualRdfTerm(toTypedLiteral(now.toISOString(), XSD.dateTime));
      expect(childMetadata.get(POSIX.mtime)).toEqualRdfTerm(toTypedLiteral(Math.floor(now.getTime() / 1000),
        XSD.integer));
    });

    it('adds stored metadata when requesting metadata.', async(): Promise<void> => {
      cache.data = { resource: 'data', 'resource.meta': '<this> <is> <metadata>.' };
      let metadata = await accessor.getMetadata({ path: `${base}resource` });
      expect(metadata.quads().some((quad): boolean => quad.subject.value === 'this'));

      cache.data = { container: { '.meta': '<this> <is> <metadata>.' }};
      metadata = await accessor.getMetadata({ path: `${base}container/` });
      expect(metadata.quads().some((quad): boolean => quad.subject.value === 'this'));
    });

    it('throws an error if there is a problem with the internal metadata.', async(): Promise<void> => {
      cache.data = { resource: 'data', 'resource.meta': 'invalid metadata!.' };
      await expect(accessor.getMetadata({ path: `${base}resource` })).rejects.toThrow();
    });
  });

  describe('writing a data resource', (): void => {
    it('throws a 404 if the identifier does not start with the base.', async(): Promise<void> => {
      await expect(accessor.writeDataResource({ path: 'badpath' }, streamifyArray([])))
        .rejects.toThrow(NotFoundHttpError);
    });

    it('throws an error when writing to a metadata path.', async(): Promise<void> => {
      await expect(accessor.writeDataResource({ path: `${base}resource.meta` }, streamifyArray([])))
        .rejects.toThrow(new ConflictHttpError('Not allowed to create files with the metadata extension.'));
    });

    it('writes the data to the corresponding file.', async(): Promise<void> => {
      const data = streamifyArray([ 'data' ]);
      await expect(accessor.writeDataResource({ path: `${base}resource` }, data)).resolves.toBeUndefined();
      expect(cache.data.resource).toBe('data');
    });

    it('writes metadata to the corresponding metadata file.', async(): Promise<void> => {
      const data = streamifyArray([ 'data' ]);
      const metadata = new RepresentationMetadata(`${base}resource`, { likes: 'apples' });
      await expect(accessor.writeDataResource({ path: `${base}resource` }, data, metadata)).resolves.toBeUndefined();
      expect(cache.data.resource).toBe('data');
      expect(cache.data['resource.meta']).toMatch(`<${base}resource> <likes> "apples".`);
    });

    it('does not write metadata that is stored by the file system.', async(): Promise<void> => {
      const data = streamifyArray([ 'data' ]);
      const metadata = new RepresentationMetadata(`${base}resource`, { [RDF.type]: toNamedNode(LDP.Resource) });
      await expect(accessor.writeDataResource({ path: `${base}resource` }, data, metadata)).resolves.toBeUndefined();
      expect(cache.data.resource).toBe('data');
      expect(cache.data['resource.meta']).toBeUndefined();
    });

    it('throws if something went wrong writing a file.', async(): Promise<void> => {
      const data = streamifyArray([ 'data' ]);
      data.read = (): any => {
        data.emit('error', new Error('error'));
        return null;
      };
      await expect(accessor.writeDataResource({ path: `${base}resource` }, data)).rejects.toThrow(new Error('error'));
    });

    it('deletes the metadata file if something went wrong writing the file.', async(): Promise<void> => {
      const data = streamifyArray([ 'data' ]);
      data.read = (): any => {
        data.emit('error', new Error('error'));
        return null;
      };
      const metadata = new RepresentationMetadata(`${base}resource`, { likes: 'apples' });
      await expect(accessor.writeDataResource({ path: `${base}resource` }, data, metadata))
        .rejects.toThrow(new Error('error'));
      expect(cache.data['resource.meta']).toBeUndefined();
    });
  });

  describe('writing a container', (): void => {
    it('throws a 404 if the identifier does not start with the base.', async(): Promise<void> => {
      await expect(accessor.writeContainer({ path: 'badpath' })).rejects.toThrow(NotFoundHttpError);
    });

    it('creates the corresponding directory.', async(): Promise<void> => {
      await expect(accessor.writeContainer({ path: `${base}container/` })).resolves.toBeUndefined();
      expect(cache.data.container).toEqual({});
    });

    it('can handle the directory already existing.', async(): Promise<void> => {
      cache.data.container = {};
      await expect(accessor.writeContainer({ path: `${base}container/` })).resolves.toBeUndefined();
      expect(cache.data.container).toEqual({});
    });

    it('throws other errors when making a directory.', async(): Promise<void> => {
      await expect(accessor.writeContainer({ path: `${base}doesntexist/container` })).rejects.toThrow();
    });

    it('writes metadata to the corresponding metadata file.', async(): Promise<void> => {
      const metadata = new RepresentationMetadata(`${base}container/`, { likes: 'apples' });
      await expect(accessor.writeContainer({ path: `${base}container/` }, metadata)).resolves.toBeUndefined();
      expect(cache.data.container).toEqual({ '.meta': expect.stringMatching(`<${base}container/> <likes> "apples".`) });
    });

    it('overwrites existing metadata.', async(): Promise<void> => {
      cache.data.container = { '.meta': `<${base}container/> <likes> "pears".` };
      const metadata = new RepresentationMetadata(`${base}container/`, { likes: 'apples' });
      await expect(accessor.writeContainer({ path: `${base}container/` }, metadata)).resolves.toBeUndefined();
      expect(cache.data.container).toEqual({ '.meta': expect.stringMatching(`<${base}container/> <likes> "apples".`) });
    });

    it('does not write metadata that is stored by the file system.', async(): Promise<void> => {
      const metadata = new RepresentationMetadata(
        `${base}container/`,
        { [RDF.type]: [ toNamedNode(LDP.BasicContainer), toNamedNode(LDP.Resource) ]},
      );
      await expect(accessor.writeContainer({ path: `${base}container/` }, metadata)).resolves.toBeUndefined();
      expect(cache.data.container).toEqual({});
    });
  });

  it('does not support modifying resources.', async(): Promise<void> => {
    await expect(accessor.modifyResource()).rejects.toThrow(new Error('Not supported.'));
  });

  describe('deleting a resource', (): void => {
    it('throws a 404 if the identifier does not start with the base.', async(): Promise<void> => {
      await expect(accessor.deleteResource({ path: 'badpath' })).rejects.toThrow(NotFoundHttpError);
    });

    it('throws a 404 if the identifier does not match an existing entry.', async(): Promise<void> => {
      await expect(accessor.deleteResource({ path: `${base}resource` })).rejects.toThrow(NotFoundHttpError);
    });

    it('throws a 404 if it matches something that is no file or directory.', async(): Promise<void> => {
      cache.data = { resource: 5 };
      await expect(accessor.deleteResource({ path: `${base}resource` })).rejects.toThrow(NotFoundHttpError);
    });

    it('throws a 404 if the trailing slash does not match its type.', async(): Promise<void> => {
      cache.data = { resource: 'apple', container: {}};
      await expect(accessor.deleteResource({ path: `${base}resource/` })).rejects.toThrow(NotFoundHttpError);
      await expect(accessor.deleteResource({ path: `${base}container` })).rejects.toThrow(NotFoundHttpError);
    });

    it('deletes the corresponding file for data resources.', async(): Promise<void> => {
      cache.data = { resource: 'apple' };
      await expect(accessor.deleteResource({ path: `${base}resource` })).resolves.toBeUndefined();
      expect(cache.data.resource).toBeUndefined();
    });

    it('throws error if there is a problem with deleting existing metadata.', async(): Promise<void> => {
      cache.data = { resource: 'apple', 'resource.meta': {}};
      await expect(accessor.deleteResource({ path: `${base}resource` })).rejects.toThrow();
    });

    it('removes the corresponding folder for containers.', async(): Promise<void> => {
      cache.data = { container: {}};
      await expect(accessor.deleteResource({ path: `${base}container/` })).resolves.toBeUndefined();
      expect(cache.data.container).toBeUndefined();
    });

    it('removes the corresponding metadata.', async(): Promise<void> => {
      cache.data = { container: { resource: 'apple', 'resource.meta': 'metaApple', '.meta': 'metadata' }};
      await expect(accessor.deleteResource({ path: `${base}container/resource` })).resolves.toBeUndefined();
      expect(cache.data.container.resource).toBeUndefined();
      expect(cache.data.container['resource.meta']).toBeUndefined();
      await expect(accessor.deleteResource({ path: `${base}container/` })).resolves.toBeUndefined();
      expect(cache.data.container).toBeUndefined();
    });
  });
});
