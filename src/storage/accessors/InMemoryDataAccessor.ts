import type { Readable } from 'stream';
import { PassThrough } from 'stream';
import arrayifyStream from 'arrayify-stream';
import { DataFactory } from 'n3';
import type { NamedNode } from 'rdf-js';
import streamifyArray from 'streamify-array';
import { RepresentationMetadata } from '../../ldp/representation/RepresentationMetadata';
import type { ResourceIdentifier } from '../../ldp/representation/ResourceIdentifier';
import { NotFoundHttpError } from '../../util/errors/NotFoundHttpError';
import type { MetadataController } from '../../util/MetadataController';
import { ensureTrailingSlash } from '../../util/Util';
import type { DataAccessor } from './DataAccessor';

interface DataEntry {
  data: Readable;
  metadata?: RepresentationMetadata;
}
interface ContainerEntry {
  entries: { [name: string]: CacheEntry };
  metadata?: RepresentationMetadata;
}
type CacheEntry = DataEntry | ContainerEntry;

export class InMemoryDataAccessor implements DataAccessor {
  private readonly base: string;
  private readonly store: ContainerEntry;
  private readonly metadataController: MetadataController;

  public constructor(base: string, metadataController: MetadataController) {
    this.base = ensureTrailingSlash(base);
    this.metadataController = metadataController;

    const metadata = new RepresentationMetadata(this.base);
    metadata.addQuads(this.metadataController.generateResourceQuads(DataFactory.namedNode(this.base), true));
    this.store = { entries: {}, metadata };
  }

  public async canHandle(): Promise<void> {
    // All data is supported since streams never get read, only copied
  }

  public async getData(identifier: ResourceIdentifier): Promise<Readable> {
    const entry = this.getEntry(identifier);
    if (!this.isDataEntry(entry)) {
      throw new NotFoundHttpError();
    }
    return this.copyData(entry);
  }

  public async getMetadata(identifier: ResourceIdentifier): Promise<RepresentationMetadata> {
    const entry = this.getEntry(identifier);
    if (this.isDataEntry(entry) === identifier.path.endsWith('/')) {
      throw new NotFoundHttpError();
    }
    return this.generateMetadata(identifier, entry);
  }

  public async getNormalizedMetadata(identifier: ResourceIdentifier): Promise<RepresentationMetadata> {
    const entry = this.getEntry(identifier);
    return this.generateMetadata(identifier, entry);
  }

  public async writeDataResource(identifier: ResourceIdentifier, data: Readable, metadata?: RepresentationMetadata):
  Promise<void> {
    const { parent, name } = this.getParentEntry(identifier);
    parent.entries[name] = {
      // Drain original stream and create copy
      data: streamifyArray(await arrayifyStream(data)),
      metadata,
    };
  }

  public async writeContainer(identifier: ResourceIdentifier, metadata?: RepresentationMetadata): Promise<void> {
    const { parent, name } = this.getParentEntry(identifier);
    parent.entries[name] = {
      entries: {},
      metadata,
    };
  }

  public async modifyResource(): Promise<void> {
    throw new Error('Not supported.');
  }

  public async deleteResource(identifier: ResourceIdentifier): Promise<void> {
    const { parent, name } = this.getParentEntry(identifier);
    if (!parent.entries[name]) {
      throw new NotFoundHttpError();
    }
    // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
    delete parent.entries[name];
  }

  private isDataEntry(entry: CacheEntry): entry is DataEntry {
    return Boolean((entry as DataEntry).data);
  }

  private getParentEntry(identifier: ResourceIdentifier): { parent: ContainerEntry; name: string } {
    let parts = identifier.path.slice(this.base.length).split('/').filter((part): boolean => part.length > 0);

    // Workaround for when identifier is root
    if (parts.length === 0) {
      return { parent: { entries: { root: this.store }}, name: 'root' };
    }

    const name = parts.slice(-1)[0];
    parts = parts.slice(0, -1);
    let parent = this.store;

    for (const part of parts) {
      const child = parent.entries[part];
      if (!child || this.isDataEntry(child)) {
        throw new NotFoundHttpError();
      }
      parent = child;
    }

    return { parent, name };
  }

  private getEntry(identifier: ResourceIdentifier): CacheEntry {
    const { parent, name } = this.getParentEntry(identifier);
    const entry = parent.entries[name];
    if (!entry) {
      throw new NotFoundHttpError();
    }
    return entry;
  }

  private copyData(source: DataEntry): Readable {
    const objectMode = { writableObjectMode: true, readableObjectMode: true };
    const streamOutput = new PassThrough(objectMode);
    const streamInternal = new PassThrough({ ...objectMode, highWaterMark: Number.MAX_SAFE_INTEGER });
    source.data.pipe(streamOutput);
    source.data.pipe(streamInternal);

    source.data = streamInternal;

    return streamOutput;
  }

  private generateMetadata(identifier: ResourceIdentifier, entry: CacheEntry): RepresentationMetadata {
    const metadata = entry.metadata ?
      new RepresentationMetadata(entry.metadata) :
      new RepresentationMetadata(identifier.path);
    if (!this.isDataEntry(entry)) {
      const childNames = Object.keys(entry.entries).map((name): string =>
        `${identifier.path}${name}${this.isDataEntry(entry.entries[name]) ? '' : '/'}`);
      const quads = this.metadataController
        .generateContainerContainsResourceQuads(metadata.identifier as NamedNode, childNames);
      metadata.addQuads(quads);
    }
    return metadata;
  }
}
