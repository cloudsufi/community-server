import streamifyArray from 'streamify-array';
import { RepresentationMetadata } from '../../../../src/ldp/representation/RepresentationMetadata';
import { InMemoryDataAccessor } from '../../../../src/storage/accessors/InMemoryDataAccessor';
import { NotFoundHttpError } from '../../../../src/util/errors/NotFoundHttpError';
import { MetadataController } from '../../../../src/util/MetadataController';
import { LDP, RDF } from '../../../../src/util/UriConstants';
import { toNamedNode } from '../../../../src/util/UriUtil';
import { readableToString } from '../../../../src/util/Util';

describe('An InMemoryDataAccessor', (): void => {
  const base = 'http://test.com/';
  let accessor: InMemoryDataAccessor;

  beforeEach(async(): Promise<void> => {
    accessor = new InMemoryDataAccessor(
      base,
      new MetadataController(),
    );
  });

  it('can only handle all data.', async(): Promise<void> => {
    await expect(accessor.canHandle()).resolves.toBeUndefined();
  });

  describe('reading and writing data', (): void => {
    it('throws a 404 if the identifier does not match an existing data resource.', async(): Promise<void> => {
      await expect(accessor.getData({ path: `${base}resource` })).rejects.toThrow(NotFoundHttpError);
      await expect(accessor.getData({ path: `${base}container/resource` })).rejects.toThrow(NotFoundHttpError);
    });

    it('throws a 404 if the identifier matches a container.', async(): Promise<void> => {
      await expect(accessor.getData({ path: base })).rejects.toThrow(NotFoundHttpError);
    });

    it('throws a 404 if part of the path matches a data resource.', async(): Promise<void> => {
      await accessor.writeDataResource({ path: `${base}resource` }, streamifyArray([ 'data' ]));
      await expect(accessor.getData({ path: `${base}resource/resource2` })).rejects.toThrow(NotFoundHttpError);
    });

    it('returns the corresponding data every time.', async(): Promise<void> => {
      const data = streamifyArray([ 'data' ]);
      await accessor.writeDataResource({ path: `${base}resource` }, data);

      // Run twice to make sure the data is stored correctly
      await expect(readableToString(await accessor.getData({ path: `${base}resource` }))).resolves.toBe('data');
      await expect(readableToString(await accessor.getData({ path: `${base}resource` }))).resolves.toBe('data');
    });
  });

  describe('reading and writing metadata', (): void => {
    it('throws a 404 if the identifier does not match an existing data resource.', async(): Promise<void> => {
      await expect(accessor.getMetadata({ path: `${base}resource` })).rejects.toThrow(NotFoundHttpError);
    });

    it('throws a 404 if the trailing slash does not match its type.', async(): Promise<void> => {
      await accessor.writeDataResource({ path: `${base}resource` }, streamifyArray([ 'data' ]));
      await expect(accessor.getMetadata({ path: `${base}resource/` })).rejects.toThrow(NotFoundHttpError);
      await accessor.writeContainer({ path: `${base}container/` });
      await expect(accessor.getMetadata({ path: `${base}container` })).rejects.toThrow(NotFoundHttpError);
    });

    it('returns metadata for incorrect trailing slashes with `getNormalizedMetadata`.', async(): Promise<void> => {
      const inputMetadata = new RepresentationMetadata(`${base}container/`, { [RDF.type]: toNamedNode(LDP.Resource) });
      await accessor.writeDataResource({ path: `${base}resource` }, streamifyArray([ 'data' ]), inputMetadata);
      let metadata = await accessor.getNormalizedMetadata({ path: `${base}resource/` });
      expect(metadata).not.toBe(inputMetadata);
      expect(metadata.quads()).toBeRdfIsomorphic(inputMetadata.quads());

      await accessor.writeContainer({ path: `${base}container/` }, inputMetadata);
      metadata = await accessor.getNormalizedMetadata({ path: `${base}resource/` });
      expect(metadata).not.toBe(inputMetadata);
      expect(metadata.quads()).toBeRdfIsomorphic(inputMetadata.quads());
    });

    it('returns empty metadata if there was none stored.', async(): Promise<void> => {
      await accessor.writeDataResource({ path: `${base}resource` }, streamifyArray([ 'data' ]));
      const metadata = await accessor.getMetadata({ path: `${base}resource` });
      expect(metadata.quads()).toHaveLength(0);
    });

    it('generates the containment metadata for a container.', async(): Promise<void> => {
      await accessor.writeContainer({ path: `${base}container/` });
      await accessor.writeDataResource({ path: `${base}container/resource` }, streamifyArray([ 'data' ]));
      await accessor.writeContainer({ path: `${base}container/container2` });
      const metadata = await accessor.getMetadata({ path: `${base}container/` });
      expect(metadata.identifier.value).toBe(`${base}container/`);
      expect(metadata.getAll(LDP.contains)).toEqualRdfTermArray(
        [ toNamedNode(`${base}container/resource`), toNamedNode(`${base}container/container2/`) ],
      );
    });

    it('adds stored metadata when requesting data resource metadata.', async(): Promise<void> => {
      const inputMetadata = new RepresentationMetadata(`${base}resource`, { [RDF.type]: toNamedNode(LDP.Resource) });
      await accessor.writeDataResource({ path: `${base}resource` }, streamifyArray([ 'data' ]), inputMetadata);
      const metadata = await accessor.getMetadata({ path: `${base}resource` });
      expect(metadata.identifier.value).toBe(`${base}resource`);
      const quads = metadata.quads();
      expect(quads).toHaveLength(1);
      expect(quads[0].object.value).toBe(LDP.Resource);
    });

    it('adds stored metadata when requesting container metadata.', async(): Promise<void> => {
      const inputMetadata = new RepresentationMetadata(`${base}container/`, { [RDF.type]: toNamedNode(LDP.Container) });
      await accessor.writeContainer({ path: `${base}container/` }, inputMetadata);
      const metadata = await accessor.getMetadata({ path: `${base}container/` });
      expect(metadata.identifier.value).toBe(`${base}container/`);
      const quads = metadata.quads();
      expect(quads).toHaveLength(1);
      expect(quads[0].object.value).toBe(LDP.Container);
    });
  });

  it('does not support modifying resources.', async(): Promise<void> => {
    await expect(accessor.modifyResource()).rejects.toThrow(new Error('Not supported.'));
  });

  describe('deleting a resource', (): void => {
    it('throws a 404 if the identifier does not match an existing entry.', async(): Promise<void> => {
      await expect(accessor.deleteResource({ path: `${base}resource` })).rejects.toThrow(NotFoundHttpError);
    });

    it('removes the corresponding resource.', async(): Promise<void> => {
      await accessor.writeDataResource({ path: `${base}resource` }, streamifyArray([ 'data' ]));
      await accessor.writeContainer({ path: `${base}container/` });
      await expect(accessor.deleteResource({ path: `${base}resource` })).resolves.toBeUndefined();
      await expect(accessor.deleteResource({ path: `${base}container/` })).resolves.toBeUndefined();
      await expect(accessor.getMetadata({ path: `${base}resource` })).rejects.toThrow(NotFoundHttpError);
      await expect(accessor.getMetadata({ path: `${base}container/` })).rejects.toThrow(NotFoundHttpError);
    });
  });
});
